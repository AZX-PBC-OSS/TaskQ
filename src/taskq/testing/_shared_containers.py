"""Shared test-container machinery: ONE Postgres + ONE Dragonfly for a whole run.

Under pytest-xdist every worker process gets its own fixture ``session`` — a
``-n 4`` run with the old per-worker session containers booted **four Postgres
plus four Dragonfly containers**, all hammering the same Docker daemon. That
contention is the measured cause of heavy PG tests (property-sweep equivalence,
state-transition equivalence, Postgres sweeps, sliding-window rate limits)
tripping internal timeouts intermittently: they passed in isolation and failed
under ``-n 4``. The design here is ported from the proven sibling implementation
(cennan), whose measured numbers this port inherits: one tuned container instead
of four contending ones, and per-worker forced ``DROP DATABASE`` checkpoints that
went from 2-14s each (worst 14.46s) to 0.05s.

How the sharing works: the first session fixture to take a file lock (under the
per-run state dir ``tmp_path_factory.getbasetemp().parent``, shared by all xdist
workers of a run) starts BOTH containers and publishes connection info to a JSON
state file; every other session fixture (in any worker) reuses it. A refcount
file tears the pair down when the last reference of the last worker releases.
Stale containers from crashed runs are removed before starting fresh ones.

The module lives in ``taskq.testing`` (not ``tests/``) because the published
fixture module :mod:`taskq.testing.fixtures` needs it — ``tests.conftest`` cannot
be imported cross-module in a pyright-resolvable way, and a bare ``conftest``
import self-shadows under ``tests/e2e/``.

Import purity: module level is stdlib-only. ``filelock``, ``docker`` and
``testcontainers`` are imported inside the functions that use them, so importing
this module (and therefore :mod:`taskq.testing.fixtures`) never requires those
packages — the same boundary rule :mod:`taskq.testing.fixtures` documents for
asyncpg/testcontainers/pytest.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Generator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

# Ryuk must not manage the shared containers: testcontainers' reaper removes a
# container when the *registering* process exits, and the creator worker can
# finish before other workers — Ryuk would reap the shared containers mid-run.
# Lifecycle is explicit instead (refcount + docker rm by the last reference to
# release). This also disables Ryuk for this process's OTHER testcontainers
# (disposable chaos containers), which is why those are labeled with
# ``creator_labels()`` too: a crashed run's leftovers must stay sweepable.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

# ============================================================================================
# Ownership labels
# ============================================================================================

CREATOR_PID_LABEL = "taskq.test.creator-pid"
CONTROLLER_PID_LABEL = "taskq.test.controller-pid"

# Any sibling repo's ownership-label key shape — see the sweep rules below. Cross-repo
# on purpose: cennan (``cennan.test.*``), warden (``warden.test.*``) and other repos on
# this shared Docker daemon label with the same key shape, and every repo's sweep honors
# ANY matching key, so one checkout's stale sweep never kills another checkout's live
# run.
OWNER_PID_LABEL_RE = re.compile(r".*\.test\.(creator|controller)-pid$")


def creator_labels() -> dict[str, str]:
    """This process's pid AND its parent's (the xdist controller when workers spawn the
    containers, the invoking shell under ``-n0``): the creating xdist WORKER can
    legitimately exit before sibling workers finish, so one pid is not enough to prove
    the owning run is alive.
    """
    return {
        CREATOR_PID_LABEL: str(os.getpid()),
        CONTROLLER_PID_LABEL: str(os.getppid()),
    }


def pid_alive(pid: int) -> bool:
    """``os.kill(pid, 0)`` liveness probe: ESRCH (``ProcessLookupError``) means dead;
    EPERM (another uid) means alive; anything else assumes alive — a sweep must never
    remove a live run's containers on a guess."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def labeled_pids(labels: Mapping[str, str]) -> list[int]:
    """The parseable pid values across ALL ownership labels (ours and any sibling
    repo's — see ``OWNER_PID_LABEL_RE``). An unparseable value cannot have come from a
    real labeling site (all write ``str(pid)``), protects no live run, and is dropped —
    it never counts as alive."""
    pids: list[int] = []
    for key, raw in labels.items():
        if not OWNER_PID_LABEL_RE.match(key):
            continue
        with suppress(ValueError):
            pids.append(int(raw))
    return pids


# ============================================================================================
# Images, commands, sweep policy
# ============================================================================================

_PG_IMAGE = "postgres:18-alpine"
_PG_USERNAME = "taskq"
_PG_PASSWORD = "taskq"  # noqa: S105 # Why: throwaway test-container credential; matches the repo's compose/test defaults.
_PG_DBNAME = "taskq"
# Server settings for the throwaway shared test cluster.
#
# ``max_connections=1000``: ONE container now serves EVERY xdist worker — ``-n auto``
# on a 32-core machine opens 32 x ~22 connections = ~700 against it, well past
# PostgreSQL's default of 100.
#
# The durability/checkpoint settings come from the measured failure mode that made the
# old per-worker sessions flaky under ``-n 4``: TaskQ's fixtures force a cluster-wide
# checkpoint on every module teardown (``DROP DATABASE ... WITH (FORCE)``) and every
# per-test reset (``DROP SCHEMA ... CASCADE``), and on a shared cluster every worker's
# forced checkpoint queues behind every other worker's. With default settings that
# checkpoint has to fsync the whole dirty buffer pool — measured on cennan's port of
# this design at 4 workers: individual drops took 2-14s (worst 14.46s, vs 0.05s tuned)
# and pushed tests past their timeout budgets. Turning durability off makes the forced
# checkpoint nearly free; ``checkpoint_timeout=3600`` keeps ordinary time-triggered
# checkpoints out of the way so the only ones that happen are the forced ones. This is
# a container that is deleted at the end of the run, so there is nothing for durability
# to protect: a crash means the data is discarded either way.
_PG_COMMAND = (
    "-c max_connections=1000"
    " -c fsync=off"
    " -c synchronous_commit=off"
    " -c full_page_writes=off"
    " -c max_wal_size=4GB"
    " -c checkpoint_timeout=3600"
)

# Dragonfly is a drop-in Redis replacement (RESP wire protocol, EVALSHA, FLUSHDB — all
# verified); pinned by tag for reproducibility.
DRAGONFLY_IMAGE = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"

# Dragonfly sizes its memory requirement as proactor_threads x 0.25GiB and EXITS at
# startup if host RAM doesn't cover it ("There are N threads, so X GiB are required.
# Exiting..."). On high-core hosts (32-core WSL2 dev boxes) that demand (8GiB) exceeds
# what the Docker VM offers, the container dies before readiness, and testcontainers
# hangs until the pytest timeout. Pin 2 threads / 512MiB: functional chaos/ratelimit
# tests need neither cores nor RAM, and the pins make startup deterministic regardless
# of host size.
DRAGONFLY_RESOURCE_FLAGS = "--proactor_threads 2 --maxmemory 512mb"

# Logical DBs available for per-module / per-test allocation (Dragonfly caps --dbnum
# at 1024; DB 0 is reserved for ad-hoc use). One per consumer across ALL workers —
# see ``next_redis_logical_db``.
REDIS_DB_POOL_SIZE = 1024

# Only containers running these EXACT images are sweep candidates: the shared pair and
# TaskQ's disposable chaos containers all use them. Deliberately not a bare
# ``postgres`` repository prefix — the docker-compose dev stack runs ``postgres:18.4``
# (same repository, different tag), and a repository-wide prefix would make the sweep
# a hazard to it (the fixed ``container_name: taskq-*`` guard below is the second line
# of defense).
_SWEEP_IMAGE_PREFIXES = (
    "postgres:18-alpine",
    "docker.dragonflydb.io/dragonflydb/",
)

# A leftover must not outlive a dead run's pid numbers forever: over days a dead run's
# pid can be recycled by an unrelated live process, so liveness alone would shield it.
# The 24h backstop bounds the leak.
SWEEP_AGE_LIMIT = timedelta(hours=24)

# The docker-compose dev stack pins ``container_name: taskq-postgres``/``taskq-redis``/
# ``taskq-admin`` — never this suite's to remove, whatever their image or state.
_PROTECTED_NAME_PREFIX = "taskq-"


def should_sweep_stale_container(
    *,
    image: str,
    name: str,
    labels: Mapping[str, str],
    running: bool,
    created: datetime,
    now: datetime,
) -> bool:
    """The keep/remove decision for one leftover container — pure except the pid-liveness
    probe (``os.kill(pid, 0)`` reads OS state), so the unit lane can test it without
    Docker (``tests/test_shared_containers.py``); the Docker I/O wrapper around it is
    exercised by every integration run.

    Rules, in order:

    1. Named ``taskq-*`` → the docker-compose dev stack — never ours to remove.
    2. Image outside ``_SWEEP_IMAGE_PREFIXES`` → not a test container this suite manages.
    3. Ownership label present (ours or any sibling repo's — ``OWNER_PID_LABEL_RE``) →
       sweep iff EVERY labeled pid is dead, or the container is older than
       ``SWEEP_AGE_LIMIT``. Two pids because the creating xdist worker can legitimately
       exit first (see ``creator_labels``). The labels are what stop this run's sweep
       from killing a CONCURRENTLY running pytest session's containers (a second
       agent's worktree checkout runs the same images): a liveness-blind sweep killing
       exactly those containers was the ~98x ConnectionRefusedError flake class in the
       sibling repo this design is ported from.
    4. Unlabeled → sweep iff it is not running, or older than ``SWEEP_AGE_LIMIT``. A
       RUNNING unlabeled container may belong to a live run of pre-label code, so it is
       left alone; exited or ancient ones are safe to remove.
    """
    if name.startswith(_PROTECTED_NAME_PREFIX):
        return False
    if not image.startswith(_SWEEP_IMAGE_PREFIXES):
        return False
    if any(OWNER_PID_LABEL_RE.match(key) for key in labels):
        any_owner_alive = any(pid_alive(pid) for pid in labeled_pids(labels))
        return (not any_owner_alive) or (now - created > SWEEP_AGE_LIMIT)
    return (not running) or (now - created > SWEEP_AGE_LIMIT)


# ============================================================================================
# Typed boundary over the untyped docker SDK
# ============================================================================================


class _DockerContainerLike(Protocol):
    """The docker SDK surface this module uses (docker-py ships no ``py.typed``, so the
    one ``cast`` at the client boundary plus this protocol keeps every downstream use
    type-checked instead of ``Unknown``)."""

    @property
    def name(self) -> str: ...

    @property
    def status(self) -> str: ...

    @property
    def labels(self) -> dict[str, str]: ...

    @property
    def attrs(self) -> dict[str, object]: ...

    def remove(self, force: bool = ...) -> None: ...


class _DockerContainersLike(Protocol):
    def get(self, container_id: str) -> _DockerContainerLike: ...

    def list(self, *, all: bool = ...) -> list[_DockerContainerLike]: ...


class _DockerClientLike(Protocol):
    @property
    def containers(self) -> _DockerContainersLike: ...


def _docker_client() -> _DockerClientLike:
    import docker

    return cast("_DockerClientLike", docker.from_env())


def _docker_errors() -> tuple[type[Exception], ...]:
    """``docker.errors.DockerException`` and its subclasses (``NotFound``, ``APIError``)
    — the specific failure modes of the calls below (missing container, daemon down,
    API error), instead of a broad swallow that would also hide bugs in this module."""
    from docker.errors import DockerException

    return (DockerException,)


# ============================================================================================
# Docker I/O wrappers (best-effort by design: a Docker hiccup must never stop a run)
# ============================================================================================


def container_running(container_id: str) -> bool:
    try:
        return _docker_client().containers.get(container_id).status == "running"
    except _docker_errors():
        return False


def cleanup_stale_testcontainers() -> None:
    """Remove stale testcontainers from crashed runs before starting fresh ones.

    The keep/remove decision lives in :func:`should_sweep_stale_container` (pure,
    unit-tested); this wrapper only does Docker I/O and never raises — a broken daemon
    or a container removed mid-list must not stop the suite starting.
    """
    try:
        containers = _docker_client().containers.list(all=True)
    except _docker_errors():
        return
    now = datetime.now(tz=UTC)
    for container in containers:
        try:
            # Config.Image is the name:tag the container was created with — no extra
            # images.get round-trip per container, and no skipped sweep when the image
            # was since deleted.
            config = cast("dict[str, object] | None", container.attrs.get("Config"))
            image = str((config or {}).get("Image") or "")
            try:
                # An unparseable timestamp is only reachable via rule 4, where age 0
                # keeps a running container (safe) and an exited one is swept anyway.
                created = datetime.fromisoformat(str(container.attrs.get("Created")))
            except ValueError:
                created = now
            if should_sweep_stale_container(
                image=image,
                name=container.name or "",
                labels=container.labels,
                running=container.status == "running",
                created=created,
                now=now,
            ):
                container.remove(force=True)
        except _docker_errors():
            continue


def start_shared_services() -> SharedServices:
    """Start the shared pair (sweeping stale containers first). Both containers carry
    the ownership labels so a FUTURE run's stale sweep can tell we are still alive and
    leave them alone (rule 3)."""
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.community.redis import RedisContainer

    cleanup_stale_testcontainers()

    owner_labels = creator_labels()
    # The postgres image entrypoint prepends ``postgres`` when the command starts with
    # ``-``, so ``-c`` flags alone are the documented form.
    pg = PostgresContainer(
        _PG_IMAGE,
        username=_PG_USERNAME,
        password=_PG_PASSWORD,  # Why: throwaway test-container credential; matches the repo's compose/test defaults.
        dbname=_PG_DBNAME,
        command=_PG_COMMAND,
    ).with_kwargs(labels=owner_labels)
    pg.start()
    redis = (
        RedisContainer(image=DRAGONFLY_IMAGE)
        # --dbnum 1024 so every consumer (module or test function, across ALL workers)
        # gets its own logical DB — the 16-DB default would force sharing, and sharing
        # lets one consumer's FLUSHDB wipe another's mid-run state.
        .with_command(f"--dbnum {REDIS_DB_POOL_SIZE} {DRAGONFLY_RESOURCE_FLAGS}")
        .with_kwargs(labels=owner_labels)
    )
    redis.start()
    return SharedServices(
        pg_dsn=pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://"),
        pg_container_id=str(pg.get_wrapped_container().id),
        redis_host=str(redis.get_container_host_ip()),
        redis_port=int(redis.get_exposed_port(6379)),
        redis_container_id=str(redis.get_wrapped_container().id),
    )


def stop_shared_services(info: SharedServices) -> None:
    try:
        containers = _docker_client().containers
    except _docker_errors():
        return
    for container_id in (info.pg_container_id, info.redis_container_id):
        try:
            containers.get(container_id).remove(force=True)
        except _docker_errors():
            continue


def services_have_live_owner(info: SharedServices) -> bool:
    """Whether the recorded containers still belong to a live test run, per their pid
    labels.

    The reuse branch below can hand a NEW run containers started by a CRASHED one (under
    ``-n0`` the state dir is shared across runs): the refcount never returns to 0 for
    the crashed run, and — worse — every labeled pid is dead, so a concurrent run's
    sweep would remove the reused containers mid-session (rule 3). Treating
    all-owners-dead as crashed and starting fresh is what keeps a reused pair owned by
    the run actually using it.

    Unlabeled running containers (started by pre-label code) are kept: there is no
    owner information to contradict the reuse. Any inspection error also keeps them —
    a docker hiccup must never break suite startup, and ``container_running`` has
    already vetted the pair.
    """
    try:
        client = _docker_client()
        saw_label = False
        pids: list[int] = []
        for container_id in (info.pg_container_id, info.redis_container_id):
            labels = client.containers.get(container_id).labels
            saw_label = saw_label or any(OWNER_PID_LABEL_RE.match(key) for key in labels)
            pids.extend(labeled_pids(labels))
        if not saw_label:
            return True
        return any(pid_alive(pid) for pid in pids)
    except _docker_errors():
        return True


# ============================================================================================
# State files, lock, refcount
# ============================================================================================


@dataclass(frozen=True)
class SharedServices:
    """Connection info for the one Postgres + one Dragonfly container pair
    that serves every test xdist worker of a run."""

    pg_dsn: str
    pg_container_id: str
    redis_host: str
    redis_port: int
    redis_container_id: str


_INFO_FILENAME = "taskq-test-services.json"
_COUNT_FILENAME = "taskq-test-services.count"
_LOCK_FILENAME = "taskq-test-services.lock"
_REDIS_DB_FILENAME = "taskq-test-services.redis-db"


def _atomic_write_text(path: Path, text: str) -> None:
    """Write via temp file + ``os.replace`` so a crash mid-write can never leave a
    truncated state file behind (a torn count/JSON file would otherwise break the next
    run's startup with an unparseable read)."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def _read_int(path: Path) -> int:
    """Tolerant read for the refcount and DB counters: missing, empty or unparseable
    yields 0. Zero is the self-healing value for both — see the pair teardown
    (``<= 0``) and the DB reset-on-pair-start semantics."""
    with suppress(OSError, ValueError):
        return int(path.read_text().strip())
    return 0


@contextmanager
def shared_service_pair(state_dir: Path) -> Generator[SharedServices, None, None]:
    """Acquire the shared Postgres + Dragonfly pair for one session-fixture lifetime.

    The first caller to take the lock starts the pair and publishes connection info
    into *state_dir* (the shared pytest tmpdir — ``tmp_path_factory.getbasetemp()
    .parent``, shared by every xdist worker of a run); later callers — in any worker —
    reuse it. A refcount tears the pair down when the last reference releases. A
    recorded pair whose labeled owners are ALL dead (a crashed earlier run) is removed
    and re-created rather than reused, and stale containers from crashed runs are
    swept before starting fresh ones.

    Both session fixtures that share the pair (``pg_container`` in ``tests/conftest.py``
    and ``redis_container`` in :mod:`taskq.testing.fixtures`) wrap this context manager
    independently, so the per-worker refcount is 2 where both are used; the pair is
    stopped exactly once, by the last release.
    """
    from filelock import FileLock

    info_path = state_dir / _INFO_FILENAME
    count_path = state_dir / _COUNT_FILENAME
    redis_db_path = state_dir / _REDIS_DB_FILENAME
    lock = FileLock(str(state_dir / _LOCK_FILENAME))

    with lock:
        info: SharedServices | None = None
        if info_path.exists():
            try:
                candidate = SharedServices(**json.loads(info_path.read_text()))
            except (ValueError, TypeError):
                candidate = None  # corrupt/torn state file: start fresh below
            if (
                candidate is not None
                and container_running(candidate.pg_container_id)
                and container_running(candidate.redis_container_id)
            ):
                if services_have_live_owner(candidate):
                    info = candidate
                else:
                    stop_shared_services(candidate)
                    info_path.unlink(missing_ok=True)
        if info is None:
            info = start_shared_services()
            _atomic_write_text(info_path, json.dumps(asdict(info)))
            _atomic_write_text(count_path, "0")
            # The Dragonfly's logical DBs die with the container: a fresh pair means a
            # fresh DB space (and serial -n0 runs share this state dir across runs, so
            # without the reset each run would march the counter toward exhaustion).
            _atomic_write_text(redis_db_path, "0")
        _atomic_write_text(count_path, str(_read_int(count_path) + 1))

    try:
        yield info
    finally:
        with lock:
            remaining = _read_int(count_path) - 1
            _atomic_write_text(count_path, str(remaining))
            # ``<= 0`` (not ``== 0``): a crash-torn count read as 0 mid-run can drive
            # the count negative; the pair must still come down, and the next run's
            # fresh start rewrites the file.
            if remaining <= 0:
                stop_shared_services(info)
                info_path.unlink(missing_ok=True)


def next_redis_logical_db(state_dir: Path) -> int:
    """The next globally-unique logical DB index on the shared Dragonfly.

    Old topology note: with per-worker containers, a per-process counter was enough —
    each worker's DBs lived in its own Dragonfly. On the ONE shared Dragonfly two
    workers' local counters would hand the SAME DB to different consumers, and one
    consumer's FLUSHDB would wipe another's mid-run state. Allocation therefore draws
    from one file-backed counter under the pair's lock, so every consumer (module or
    test function) across EVERY worker is unique. The counter is reset when a fresh
    pair starts (the DBs die with the container) and never wraps around — exhaustion
    raises loudly instead of silently sharing.
    """
    from filelock import FileLock

    lock = FileLock(str(state_dir / _LOCK_FILENAME))
    with lock:
        path = state_dir / _REDIS_DB_FILENAME
        db = _read_int(path) + 1
        if db >= REDIS_DB_POOL_SIZE:
            msg = f"exhausted Redis logical DBs ({REDIS_DB_POOL_SIZE})"
            raise RuntimeError(msg)
        _atomic_write_text(path, str(db))
        return db
