"""Build the examples Compose stack's image via containerspec.

The examples Compose stack (``examples/docker-compose.yml``) builds its image
from the repository root with ``examples/Dockerfile`` — a build TaskQ's
compose E2E (``tests/test_compose_stack_e2e.py``) and every local
``cd examples && docker compose up`` pay. Expressed as a containerspec
``ImageSpec``, the image is content-hashed: the tag pins the spec's content
(``taskq-example:sha-<16 hex>``), so a rebuild after a no-op change is
skipped entirely — even when BuildKit's layer cache is gone (pruned, or a
fresh CI runner), because the image itself is the cache key.

Measured (2026-09, see ``perf-evidence-containerspec-image.md``): true-cold
build is ~9 s slower via the spec (staging + digest + ``--pull``), the
repeated-run path drops from ~2.4-5.5 s (warm ``docker compose build``) to
~1.2 s (tag-exists skip), and after a builder-cache eviction from ~36 s to
the same ~1.2 s. The spec-built image is byte-equal to the Dockerfile build
modulo documented build-timestamp artifacts.

Usage (the compose file picks the image up via ``TASKQ_EXAMPLE_IMAGE``)::

    export TASKQ_EXAMPLE_IMAGE="$(uv run python benchmarks/example_image_spec.py)"
    docker compose -f examples/docker-compose.yml up -d   # no --build: reuses the hashed image

``--print-tag`` prints the tag without building (pure Python, no Docker).

The build context is staged, not used in place: containerspec hashes the
copy layer's source path string AND its bytes, so the staging directory must
be stable across runs (a per-run temp path would re-mint the hash and pay a
full rebuild every run — the same lesson ``tests/e2e/conftest.py``'s wheel
cache learned). Staging also replicates the root ``.dockerignore``'s
root-anchored semantics (Docker's dockerignore is not gitignore: a bare
``__pycache__`` excludes only the root-level dir, nested ones stay in the
context — verified against this daemon's build), which is what makes the
spec-built image byte-equal to ``docker build -f examples/Dockerfile .``.
"""

from __future__ import annotations

import argparse
import asyncio
import fnmatch
import hashlib
import os
import shutil
import sys
import tempfile
from pathlib import Path

from containerspec import DockerTarget, ImageSpec

_REPO_ROOT = Path(__file__).resolve().parents[1]

#: Repository name for the content-hashed image.
IMAGE_REPOSITORY = "taskq-example"

#: Stable staging root OUTSIDE the repository (the staged context is a full
#: copy of it — inside the repo it would feed the next context walk into
#: itself).
_CONTEXT_CACHE_ROOT = Path.home() / ".cache" / "taskq" / "docker-contexts"


def _dockerignore_patterns(repo_root: Path) -> list[str]:
    """Non-comment patterns from the root .dockerignore, in file order."""
    patterns: list[str] = []
    for line in (repo_root / ".dockerignore").read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            patterns.append(stripped)
    return patterns


def _context_entries(repo_root: Path) -> list[tuple[str, Path]]:
    """Walk ``repo_root`` the way ``docker build`` assembles its context.

    Root-anchored pattern match (``fnmatch`` on the repo-relative path), with
    an excluded directory pruning its whole subtree — Docker re-includes
    nothing under an excluded dir.
    """
    patterns = _dockerignore_patterns(repo_root)
    excluded_prefixes: list[str] = []

    def excluded(rel: str) -> bool:
        if any(fnmatch.fnmatchcase(rel, pat) for pat in patterns):
            return True
        return any(rel.startswith(prefix) for prefix in excluded_prefixes)

    entries: list[tuple[str, Path]] = []
    for path in sorted(repo_root.rglob("*")):
        rel = path.relative_to(repo_root).as_posix()
        if excluded(rel):
            if path.is_dir():
                excluded_prefixes.append(rel + "/")
            continue
        entries.append((rel, path))
    return entries


def _context_digest(repo_root: Path) -> str:
    """16-hex digest of the context's paths + file bytes (cache-busts on any
    content change; recomputed every call so a source edit is never stale)."""
    digest = hashlib.sha256()
    for rel, path in _context_entries(repo_root):
        if not path.is_file():
            continue
        digest.update(rel.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def stage_context(repo_root: Path = _REPO_ROOT, cache_root: Path = _CONTEXT_CACHE_ROOT) -> Path:
    """Stage the build context at a stable, content-addressed path.

    The digest is recomputed on every call (so an edited source always mints
    a new staging entry and never serves stale bytes); the copy itself is
    reused when that entry already exists, which is what keeps the spec's
    content hash — and therefore the image tag — stable across runs.
    """
    digest = _context_digest(repo_root)
    target = cache_root / "example" / digest
    if target.exists():
        return target
    # The scratch dir must share the target's filesystem for the atomic
    # os.replace (a /tmp scratch is cross-device when the cache lives in $HOME).
    target.parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="taskq-example-ctx-", dir=target.parent))
    try:
        ctx = scratch / "ctx"
        for rel, path in _context_entries(repo_root):
            dest = ctx / rel
            if path.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            elif path.is_symlink():
                dest.parent.mkdir(parents=True, exist_ok=True)
                os.symlink(os.readlink(path), dest)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dest, follow_symlinks=False)
        try:
            os.replace(ctx, target)  # atomic on one filesystem
            scratch.rmdir()
        except OSError:
            shutil.rmtree(scratch, ignore_errors=True)  # lost a concurrent race
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    if not target.exists():
        raise RuntimeError(f"staged context missing after staging race: {target}")
    return target


def example_image_spec(repo_root: Path = _REPO_ROOT) -> ImageSpec:
    """``examples/Dockerfile`` as an ImageSpec chain (byte-equal output).

    The original::

        FROM python:3.12-slim
        RUN pip install --no-cache-dir uv
        WORKDIR /app
        COPY . /app
        RUN uv sync --extra fastapi --extra redis

    ``pip_install`` renders the same ``pip install --no-cache-dir``; the COPY
    source is the staged context (the round-trip's only non-mechanical diff).
    """
    context = stage_context(repo_root)
    return (
        ImageSpec.from_registry("python:3.12-slim", pin_digest=False)
        .pip_install("uv")
        .workdir("/app")
        .copy(str(context), "/app")
        .run_commands("uv sync --extra fastapi --extra redis")
    )


def build_example_image(repo_root: Path = _REPO_ROOT) -> str:
    """Build (or reuse by content hash) the examples image; return its tag."""
    spec = example_image_spec(repo_root)
    built = asyncio.run(spec.build(DockerTarget(IMAGE_REPOSITORY)))
    return built.tag


def print_tag(repo_root: Path = _REPO_ROOT) -> str:
    """The content-hashed tag, daemonless (pure hashing, no build)."""
    return example_image_spec(repo_root).tag(IMAGE_REPOSITORY)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the examples compose image via containerspec."
    )
    parser.add_argument(
        "--print-tag",
        action="store_true",
        help="print the content-hashed tag without building (no Docker needed)",
    )
    args = parser.parse_args()
    tag = print_tag() if args.print_tag else build_example_image()
    print(tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
