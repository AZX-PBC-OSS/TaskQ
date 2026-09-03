"""Worker-image hygiene for the e2e tier: pid-owned naming, sweeps, teardown.

Why this module exists — the image-pollution mechanics it closes:

``e2e_worker_image`` (conftest) builds one ~277 MB worker image per session via
containerspec. Two defects made that a permanent leak, one image per run:

1. Nothing ever removed the image. Teardown deleted only the wheel's build
   dir, so every unique ``taskq-e2e-worker:sha-<content hash>`` tag stranded.
2. The content hash covered the wheel copy layer's ``src`` PATH STRING, and
   that path was pid-unique (``dist-e2e-<pid>/``). Every run therefore minted
   a NEW hash — and a new image — even with byte-identical wheels and zero
   source edits, and paid a full pip-install rebuild on top (the cross-run
   layer caching the fixture docstring promised never actually engaged).

The design here (naming in this module, wiring in conftest):

- The image is built under a PID-OWNED repository name,
  ``taskq-e2e-worker-r<pid>``, so this session's tag is exclusively its own.
  No concurrent session can be using it — even one with byte-identical
  source, whose tag carries *their* pid — which is what makes
  ``remove_worker_image`` safe to call blind in teardown: it removes exactly
  this run's image and nothing any other run owns. The repository still
  begins with ``taskq-e2e-worker``, so worker CONTAINERS remain sweep
  candidates for ``taskq.testing._shared_containers`` (its
  ``_SWEEP_IMAGE_PREFIXES`` entry matches by prefix).
- Session start sweeps crashed runs' leftovers, mirroring the stale-network
  sweep's rule: the pid suffix IS the owner identity, dead owner ⇒ remove,
  with the shared 24 h age backstop against pid recycling.
- The wheel itself now lives at a stable content-addressed path
  (``dist-e2e-wheels/<sha16>/`` — see ``_build_wheel`` in conftest), so the
  content hash and the BuildKit layer cache are stable across runs.

Legacy ``taskq-e2e-worker:sha-*`` images (built before pid ownership) record
no owner in their name, so the automatic sweep deliberately leaves them
alone — a concurrent checkout running older code can still build into that
name. ``make clean-e2e`` (``scripts/clean_e2e.py``) owns removing them.

Import purity: the docker SDK is imported inside the functions that use it,
matching ``taskq.testing._shared_containers``' boundary rule. The Docker I/O
wrappers are best-effort by design — a Docker hiccup must never fail a test
session — while the keep/remove decisions are pure and unit-tested in
``test_image_hygiene.py`` (the same split ``tests/test_shared_containers.py``
applies to ``taskq.testing._shared_containers``).
"""

from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

from taskq.testing._shared_containers import (
    SWEEP_AGE_LIMIT,
    _created_or_now,
    cleanup_stale_testcontainers,
    pid_alive,
)

__all__ = [
    "WHEEL_CACHE_DIR",
    "WORKER_IMAGE_PREFIX",
    "clean_e2e_strays",
    "remove_worker_image",
    "should_sweep_stale_wheel_cache_entry",
    "should_sweep_stale_wheel_scratch_dir",
    "should_sweep_stale_worker_image",
    "sweep_stale_wheel_cache_entries",
    "sweep_stale_wheel_scratch_dirs",
    "sweep_stale_worker_images",
    "worker_image_target",
]

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Every worker image repository starts with this prefix. It is also the
# entry taskq.testing._shared_containers._SWEEP_IMAGE_PREFIXES uses to treat
# worker CONTAINERS as sweep candidates — a property pinned by
# test_image_hygiene.py (worker_image_target keeps the prefix).
WORKER_IMAGE_PREFIX = "taskq-e2e-worker"

# Stable, content-addressed cache for the wheels the worker image installs
# (populated by _build_wheel in conftest; gitignored via the existing
# ``dist-e2e-*/`` pattern). One SUBDIR per content digest, the canonical
# wheel filename inside — pip validates wheel filenames against the wheel's
# own metadata, so the digest cannot ride in the filename. Entries carry no
# owner identity — identical content is legitimately reusable by any
# session — so only the age backstop applies.
WHEEL_CACHE_DIR = _REPO_ROOT / "dist-e2e-wheels"

_PID_OWNED_REPOSITORY_RE = re.compile(rf"^{re.escape(WORKER_IMAGE_PREFIX)}-r(\d+)$")
_WHEEL_SCRATCH_DIR_RE = re.compile(r"^dist-e2e-(\d+)$")


def worker_image_target(pid: int) -> str:
    """The pid-owned DockerTarget repository name for one test process.

    Pid ownership is what lets teardown remove this session's image blind:
    the tag is this process's alone (conftest builds under
    ``worker_image_target(os.getpid())``), while the shared prefix keeps the
    image's containers sweepable and the content hash (now pid-free — see
    ``_build_wheel``) keeps BuildKit's layer cache warm across runs.
    """
    return f"{WORKER_IMAGE_PREFIX}-r{pid}"


def should_sweep_stale_worker_image(*, repository: str, created: datetime, now: datetime) -> bool:
    """Keep/remove decision for one worker-image repository — the image twin
    of ``taskq.testing._shared_containers.should_sweep_stale_network``: the
    pid suffix IS the owner identity, so sweep iff that pid is dead, with
    the 24 h age backstop against pid recycling.

    Legacy exact-name repositories and any other name shape are NEVER
    auto-swept: a legacy name records no owner, and a concurrent checkout
    running pre-ownership code can still be building into it —
    ``make clean-e2e`` owns those instead.
    """
    match = _PID_OWNED_REPOSITORY_RE.fullmatch(repository)
    if match is None:
        return False
    return (not pid_alive(int(match.group(1)))) or (now - created > SWEEP_AGE_LIMIT)


def should_sweep_stale_wheel_scratch_dir(*, name: str, created: datetime, now: datetime) -> bool:
    """Keep/remove decision for one wheel scratch dir (``dist-e2e-<pid>`` —
    the pid-unique ``uv build --out-dir`` of a run's ``_build_wheel``).

    Same rule as :func:`should_sweep_stale_worker_image`: the pid suffix is
    the owner identity; sweep iff dead, with the age backstop. The
    content-addressed :data:`WHEEL_CACHE_DIR` and any non-``dist-e2e-<pid>``
    name are not scratch dirs and are never matched.
    """
    match = _WHEEL_SCRATCH_DIR_RE.fullmatch(name)
    if match is None:
        return False
    return (not pid_alive(int(match.group(1)))) or (now - created > SWEEP_AGE_LIMIT)


def should_sweep_stale_wheel_cache_entry(*, created: datetime, now: datetime) -> bool:
    """Keep/remove decision for one cached wheel entry (the content-addressed
    ``dist-e2e-wheels/<sha16>/`` subdir holding the canonical-named wheel).

    No owner identity exists to consult: the entry's name is its content
    digest, and the same content is legitimately reusable by any live
    session (a fresh one simply rebuilds and re-places it atomically). Only
    the age backstop applies — bounded at the same 24 h the container sweep
    uses, and safe for the same reason: no e2e session outlives it.
    """
    return now - created > SWEEP_AGE_LIMIT


# ── Docker I/O wrappers (best-effort: never raise) ─────────────────────────


def _worker_image_entries() -> list[tuple[str, datetime]]:
    """``(tag, created)`` for every image whose repository starts with the
    worker prefix. Best-effort: a Docker error yields ``[]``.

    One listing call for both sweeps and the manual clean; the created
    timestamp parses through the shared sweep's tolerant
    ``_created_or_now`` (Docker emits RFC 3339 with nanosecond precision,
    and a missing/unparseable value reads as *now* — age 0 keeps, which is
    the fail-safe direction for removal).
    """
    import docker
    from docker.errors import DockerException

    try:
        images = docker.from_env().images.list()
    except DockerException:
        return []
    now = datetime.now(tz=UTC)
    entries: list[tuple[str, datetime]] = []
    for image in images:
        try:
            created = _created_or_now(image.attrs, now)
            entries.extend(
                (tag, created)
                for tag in image.tags
                if tag.rsplit(":", 1)[0].startswith(WORKER_IMAGE_PREFIX)
            )
        except DockerException:
            continue
    return entries


def sweep_stale_worker_images() -> int:
    """Session-start sweep: remove pid-owned worker images whose owner pid
    is dead (crashed runs' leftovers). Never raises; per-image Docker errors
    (e.g. an image a leftover container still references) skip that image
    for a later sweep. Returns how many were removed.
    """
    import docker
    from docker.errors import DockerException

    try:
        client = docker.from_env()
    except DockerException:
        return 0
    now = datetime.now(tz=UTC)
    swept = 0
    for tag, created in _worker_image_entries():
        if not should_sweep_stale_worker_image(
            repository=tag.rsplit(":", 1)[0], created=created, now=now
        ):
            continue
        try:
            # Non-force on purpose: refuses (and defers to a later sweep)
            # while any container — even a stopped leftover — references it.
            client.images.remove(tag)
            swept += 1
        except DockerException:
            continue
    return swept


def sweep_stale_wheel_scratch_dirs() -> int:
    """Session-start sweep: remove dead-owner wheel scratch dirs. Never
    raises; filesystem races (a dir vanishing mid-iteration) skip it.
    """
    now = datetime.now(tz=UTC)
    swept = 0
    for path in _REPO_ROOT.glob("dist-e2e-*"):
        try:
            if not path.is_dir() or not should_sweep_stale_wheel_scratch_dir(
                name=path.name, created=_mtime_as_datetime(path), now=now
            ):
                continue
            shutil.rmtree(path, ignore_errors=True)
            swept += 1
        except OSError:
            continue
    return swept


def sweep_stale_wheel_cache_entries() -> int:
    """Session-start sweep: age out stale cached wheel entries. Never
    raises; filesystem races (an entry vanishing mid-iteration) skip it.

    An entry is normally a content-addressed subdir, but any stray shape
    (a file a crashed or older run left directly in the cache dir) is aged
    out by the same rule — the dir is a pure cache, repopulated on demand.
    """
    now = datetime.now(tz=UTC)
    swept = 0
    for entry in WHEEL_CACHE_DIR.glob("*"):
        try:
            if not should_sweep_stale_wheel_cache_entry(created=_mtime_as_datetime(entry), now=now):
                continue
            _remove_path(entry)
            swept += 1
        except OSError:
            continue
    return swept


def remove_worker_image(tag: str) -> bool:
    """Teardown removal of exactly this session's worker image tag.

    Safe to call blind: the pid-owned repository name guarantees no other
    session can hold this tag (see :func:`worker_image_target`). Non-force,
    so an image some leftover container still references is refused rather
    than ripped out — it then falls to the next session-start sweep. Best-
    effort: a failure is reported on one ``[TaskQ]`` line, never raised.
    """
    import docker
    from docker.errors import DockerException

    try:
        docker.from_env().images.remove(tag)
    except DockerException as exc:
        print(f"[TaskQ] event=e2e-worker-image-removal-failed tag={tag} error={exc!r}")
        return False
    return True


def _mtime_as_datetime(path: Path) -> datetime:
    """The path's mtime as an aware datetime — the age input for the
    filesystem sweep rules (dirs and cached wheels)."""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _remove_path(path: Path) -> None:
    """Remove a cache entry of either shape: rmtree for a directory, unlink
    for a stray file. Raises OSError only if the entry vanishes mid-call
    (a concurrent sweep) — callers treat that as already-removed."""
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


# ── Manual cleanup (make clean-e2e) ────────────────────────────────────────


def clean_e2e_strays() -> None:
    """The ``make clean-e2e`` body: remove every e2e stray this tier can
    leave on a machine. Unlike the session-start sweeps this is
    user-invoked and verbose — one line per decision.

    Removes, in order:

    1. Stale test containers + pid-suffixed networks — the standard sweep
       every e2e run starts with (:func:`cleanup_stale_testcontainers`).
    2. Worker images this tier built: pid-owned repositories whose owner
       pid is dead, plus LEGACY ``taskq-e2e-worker:sha-*`` images from
       pre-ownership runs (no owner is recorded in those names, so the
       automatic sweep must leave them; here they are unambiguously this
       tier's strays). Removal is non-force: any image a container still
       references is refused and reported, never ripped out.
    3. Cached wheel entries (``dist-e2e-wheels/<sha16>/``) — a manual clean
       wants the space back; the next run rebuilds what it needs.
    4. Dead-owner wheel scratch dirs (``dist-e2e-<pid>/``).

    Resources whose owner pid is alive — a concurrently running e2e session
    — are skipped and reported, never removed.
    """
    import docker
    from docker.errors import DockerException

    cleanup_stale_testcontainers()

    try:
        client = docker.from_env()
    except DockerException as exc:
        print(f"[TaskQ] clean-e2e: docker unavailable ({exc!r}) — image pass skipped")
        client = None
    now = datetime.now(tz=UTC)

    removed = 0
    skipped = 0
    if client is not None:
        for tag, created in _worker_image_entries():
            repository = tag.rsplit(":", 1)[0]
            if _PID_OWNED_REPOSITORY_RE.fullmatch(repository):
                if not should_sweep_stale_worker_image(
                    repository=repository, created=created, now=now
                ):
                    print(f"[TaskQ] clean-e2e: keeping image {tag} (owner pid alive)")
                    skipped += 1
                    continue
            elif repository != WORKER_IMAGE_PREFIX:
                print(f"[TaskQ] clean-e2e: keeping image {tag} (unrecognized name)")
                skipped += 1
                continue
            try:
                client.images.remove(tag)
                print(f"[TaskQ] clean-e2e: removed image {tag}")
                removed += 1
            except DockerException as exc:
                print(f"[TaskQ] clean-e2e: could not remove {tag} ({exc!r})")
                skipped += 1

    wheels = 0
    for entry in WHEEL_CACHE_DIR.glob("*"):
        try:
            _remove_path(entry)
            wheels += 1
        except OSError:
            continue
    if wheels:
        print(
            f"[TaskQ] clean-e2e: removed {wheels} cached wheel entry/entries "
            f"from {WHEEL_CACHE_DIR.name}/"
        )

    scratch = 0
    for path in _REPO_ROOT.glob("dist-e2e-*"):
        if path == WHEEL_CACHE_DIR or not path.is_dir():
            continue
        try:
            stale = should_sweep_stale_wheel_scratch_dir(
                name=path.name, created=_mtime_as_datetime(path), now=now
            )
        except OSError:
            continue
        if stale:
            shutil.rmtree(path, ignore_errors=True)
            print(f"[TaskQ] clean-e2e: removed scratch dir {path.name}/")
            scratch += 1
        else:
            print(f"[TaskQ] clean-e2e: keeping scratch dir {path.name}/ (owner alive)")
            skipped += 1

    print(
        "[TaskQ] clean-e2e: done "
        f"images_removed={removed} images_kept={skipped} wheels_removed={wheels} "
        f"scratch_dirs_removed={scratch}"
    )
