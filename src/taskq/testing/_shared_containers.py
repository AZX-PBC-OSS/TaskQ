"""Shared test-container machinery: ONE Postgres + ONE Dragonfly per pytest invocation.

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

Blast-radius trade-off, stated next to that tuning rationale: ONE shared
container concentrates the failure signature of an out-of-band ``docker rm``
or daemon crash onto every worker at once — an instrumented kill of the shared
pair errored ~84 tests across every PG module, vs ~1/4 of the run per worker
under the old per-worker topology. That concentration is inherent to sharing
and accepted for the contention win above; the ``[TaskQ]`` decision logs below
(``pair-started`` / ``pair-reused`` / ``pair-fresh-started`` / ``pair-stopped``
/ ``swept``) exist so such an event is diagnosable from the run's output.

How the sharing works: the first session fixture to take a file lock (under the
per-invocation state dir — :func:`invocation_state_dir`, shared by every xdist
worker of the invocation and visible to no other invocation, of this repo or
any other) starts BOTH containers and publishes connection info to a JSON state
file; every other session fixture (in any worker of the invocation) reuses it.
References are tracked as HOLDER PIDS in a per-invocation registry — not as a
bare counter — so the pair comes down when the last LIVE holder of the
invocation releases, and a killed worker neither wedges it up forever nor gets
it torn down under a surviving sibling. Stale containers from crashed runs are
removed before starting fresh ones, by pid-label liveness alone (see the sweep
rules).

The module lives in ``taskq.testing`` (not ``tests/``) because the published
fixture module :mod:`taskq.testing.fixtures` needs it — ``tests.conftest``
cannot be imported cross-module in a pyright-resolvable way, and a bare ``conftest``
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
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

# Ryuk must not manage the shared containers: testcontainers' reaper removes a
# container when the *registering* process exits, and the creator worker can
# finish before other workers — Ryuk would reap the shared containers mid-run.
# Lifecycle is explicit instead (holder registry + docker rm by the last live
# reference to release). This also disables Ryuk for this process's OTHER testcontainers
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
# at 1024; DB 0 is reserved for ad-hoc use). The budget is per PAIR — and the pair is
# per invocation, so it sizes one invocation's consumers, one DB each across all of
# its workers — see ``next_redis_logical_db``.
REDIS_DB_POOL_SIZE = 1024

# Only containers running these EXACT images are sweep candidates: the shared pair and
# TaskQ's disposable chaos containers all use them. Deliberately not a bare
# ``postgres`` repository prefix — the docker-compose dev stack runs ``postgres:18.4``
# (same repository, different tag), and a repository-wide prefix would make the sweep
# a hazard to it (the fixed ``container_name: taskq-*`` guard below is the second line
# of defense).
#
# ``taskq-e2e-worker`` (tagged ``taskq-e2e-worker-r<pid>:sha-<hash>`` by
# containerspec — pid-owned repository names, so each session's teardown
# removes exactly its own image) is in by IMAGE PREFIX rather than via a
# generic ``taskq.test-managed`` label honored
# by the sweep: the name is a repo-owned constant, so this one entry makes every e2e
# worker-container creation site (a dozen across tests/e2e, and any future one) a
# sweep candidate with no per-site opt-in to forget — the exact labeling omission that
# left crashed e2e runs' worker containers unsweepable in the first place. It is safe
# alongside the compose guard: the dev stack's containers are name-protected
# (``taskq-*``) and run different images.
_SWEEP_IMAGE_PREFIXES = (
    "postgres:18-alpine",
    "docker.dragonflydb.io/dragonflydb/",
    "taskq-e2e-worker",
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
    3. Older than ``SWEEP_AGE_LIMIT`` → swept whatever the liveness signals say. Both
       of those signals are pids, and over days a dead run's pid can be recycled by an
       unrelated live process; the age backstop is what stops such a phantom shielding
       a leftover forever.
    4. Ownership label present (ours or any sibling repo's — ``OWNER_PID_LABEL_RE``) →
       sweep iff EVERY labeled pid is dead. Two pids because the creating xdist worker
       can legitimately exit first (see ``creator_labels``). The labels alone carry the
       cross-invocation guarantee, by construction: a pair belongs to exactly one
       invocation (:func:`invocation_state_dir` — no other invocation can even find its
       state file), and while that invocation is alive, the creating worker or the
       xdist controller (under ``-n0``, the pytest process itself) is alive too. All
       labeled pids dead therefore means no live invocation of ANY repo is using the
       container — the foreign label shapes are honored for exactly that reason. A
       liveness-blind sweep killing concurrently running sessions' containers was the
       ~98x ConnectionRefusedError flake class in the sibling repo this design is
       ported from.
    5. Unlabeled → sweep iff it is not running. A RUNNING unlabeled container may
       belong to a live run of pre-label code, so it is left alone; exited ones are
       safe to remove.
    """
    if name.startswith(_PROTECTED_NAME_PREFIX):
        return False
    if not image.startswith(_SWEEP_IMAGE_PREFIXES):
        return False
    if now - created > SWEEP_AGE_LIMIT:
        return True
    if any(OWNER_PID_LABEL_RE.match(key) for key in labels):
        return not any(pid_alive(pid) for pid in labeled_pids(labels))
    return not running


# e2e sessions create one pid-suffixed Docker network per test process
# (``taskq-e2e-net-<pid>``); a crashed run leaks it — networks are outside
# Ryuk's reap even when enabled, and Ryuk is disabled here anyway.
_E2E_NETWORK_NAME_RE = re.compile(r"^taskq-e2e-net-(\d+)$")


def should_sweep_stale_network(*, name: str, created: datetime, now: datetime) -> bool:
    """The keep/remove decision for one leftover e2e Docker network — pure
    except the pid-liveness probe, mirroring :func:`should_sweep_stale_container`
    so the unit lane can test it without Docker.

    The pid suffix IS the owner identity (the e2e suite mints it from the
    test process's own pid): sweep iff that pid is dead, with the same 24h
    age backstop against pid recycling. Names outside the exact pattern —
    Docker's own bridges, the compose dev stack's networks — are never this
    suite's to remove, however old.
    """
    match = _E2E_NETWORK_NAME_RE.fullmatch(name)
    if match is None:
        return False
    return (not pid_alive(int(match.group(1)))) or (now - created > SWEEP_AGE_LIMIT)


# ============================================================================================
# Typed boundary over the untyped docker SDK
# ============================================================================================


class _DockerContainerLike(Protocol):
    """The docker SDK surface this module uses (docker-py ships no ``py.typed``, so the
    one ``cast`` at the client boundary plus this protocol keeps every downstream use
    type-checked instead of ``Unknown``)."""

    @property
    def id(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def status(self) -> str: ...

    @property
    def labels(self) -> dict[str, str]: ...

    @property
    def attrs(self) -> dict[str, object]: ...

    def remove(self, force: bool = ...) -> None: ...


class _DockerNetworkLike(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def attrs(self) -> dict[str, object]: ...

    def remove(self) -> None: ...


class _DockerContainersLike(Protocol):
    def get(self, container_id: str) -> _DockerContainerLike: ...

    def list(self, *, all: bool = ...) -> list[_DockerContainerLike]: ...


class _DockerNetworksLike(Protocol):
    def list(self) -> list[_DockerNetworkLike]: ...


class _DockerClientLike(Protocol):
    @property
    def containers(self) -> _DockerContainersLike: ...

    @property
    def networks(self) -> _DockerNetworksLike: ...


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


def _created_or_now(attrs: Mapping[str, object], now: datetime) -> datetime:
    """The ``Created`` attr, or *now* when missing/unparseable — age 0 keeps a
    running unlabeled container (safe) and an exited one is swept anyway."""
    try:
        return datetime.fromisoformat(str(attrs.get("Created")))
    except ValueError:
        return now


def _remove_stale_networks(client: _DockerClientLike, now: datetime) -> int:
    """Best-effort removal of stale e2e networks (decision in
    :func:`should_sweep_stale_network`). Never raises: a network with
    endpoints still attached (a live run's, or one whose leftover containers
    were removed only moments before) refuses removal with a Docker API error
    and is simply left for a later sweep."""
    try:
        networks = client.networks.list()
    except _docker_errors():
        return 0
    swept = 0
    for network in networks:
        try:
            if should_sweep_stale_network(
                name=network.name or "",
                created=_created_or_now(network.attrs, now),
                now=now,
            ):
                network.remove()
                swept += 1
        except _docker_errors():
            continue
    return swept


def cleanup_stale_testcontainers() -> None:
    """Remove stale testcontainers AND stale e2e networks from crashed runs
    before starting fresh ones.

    The keep/remove decisions live in :func:`should_sweep_stale_container`
    and :func:`should_sweep_stale_network` (pure, unit-tested — the container
    rules consult pid labels, age and running state only, never a registry);
    this wrapper only does Docker I/O and never raises — a broken daemon or a
    container removed mid-list must not stop the suite starting. One ``[TaskQ]``
    ``event=swept`` line records that the sweep ran and what it removed (its
    ABSENCE means a Docker error short-circuited the sweep before the list).
    """
    try:
        client = _docker_client()
        containers = client.containers.list(all=True)
    except _docker_errors():
        return
    now = datetime.now(tz=UTC)
    swept_containers = 0
    for container in containers:
        try:
            # Config.Image is the name:tag the container was created with — no extra
            # images.get round-trip per container, and no skipped sweep when the image
            # was since deleted.
            config = cast("dict[str, object] | None", container.attrs.get("Config"))
            image = str((config or {}).get("Image") or "")
            if should_sweep_stale_container(
                image=image,
                name=container.name or "",
                labels=container.labels,
                running=container.status == "running",
                created=_created_or_now(container.attrs, now),
                now=now,
            ):
                container.remove(force=True)
                swept_containers += 1
        except _docker_errors():
            continue
    swept_networks = _remove_stale_networks(client, now)
    print(f"[TaskQ] event=swept containers={swept_containers} networks={swept_networks}")


def start_shared_services() -> SharedServices:
    """Start the shared pair (sweeping stale containers first). Both containers carry
    the ownership labels so a FUTURE run's stale sweep can tell we are still alive and
    leave them alone (rule 4)."""
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

    Secondary to the holder registry, which knows about every live holder of the
    invocation — including workers that never started the pair: this answers only
    "did the invocation that started these containers die". It still earns its place
    as the fallback for a lost registry file and for the window between starting the
    containers and registering the first holder.

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
# State files, lock
# ============================================================================================


class _TempPathFactoryLike(Protocol):
    """The ``pytest.TempPathFactory`` surface the state-dir resolver needs —
    structural, so the real factory satisfies it and unit tests can pass a
    controlled basetemp without building a pytest ``Config``."""

    def getbasetemp(self) -> Path: ...


def invocation_state_dir(factory: _TempPathFactoryLike) -> Path:
    """The per-invocation state dir for the shared pair, from pytest's basetemp.

    Invariant: an xdist worker's basetemp is ``<invocation>/popen-gwK`` — one
    level below the invocation dir — while a serial run's basetemp IS the
    invocation dir (``pytest-N``); pytest allocates a fresh numbered dir per
    invocation and never reuses one. So the parent of a worker basetemp, or a
    serial basetemp itself, is a directory exactly ONE invocation owns: the
    pair's state files and holder registry live there, and no other invocation
    — of this repo or any other — can find them. A custom ``--basetemp`` that
    names the same path for two runs opts into that sharing explicitly, the
    same way it opts into sharing every ``tmp_path``.
    """
    basetemp = factory.getbasetemp()
    return basetemp.parent if basetemp.name.startswith("popen-") else basetemp


@dataclass(frozen=True)
class SharedServices:
    """Connection info for the one Postgres + one Dragonfly container pair
    that serves every xdist worker of one pytest invocation."""

    pg_dsn: str
    pg_container_id: str
    redis_host: str
    redis_port: int
    redis_container_id: str


_INFO_FILENAME = "taskq-test-services.json"
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
    """Tolerant read for the logical-DB counter: missing, empty or unparseable yields
    0 — the self-healing value, matching the reset-on-pair-start semantics."""
    with suppress(OSError, ValueError):
        return int(path.read_text().strip())
    return 0


# ============================================================================================
# Holder registry (per invocation)
# ============================================================================================
#
# Why a registry at all, when the containers already carry pid labels: the labels
# prove the OWNING invocation is alive (the creating worker and the xdist controller
# it recorded — see ``creator_labels``), but not HOW MANY session fixtures of that
# invocation still hold the pair. The creating worker can exit before its siblings
# finish, and the pair must come down exactly when the LAST holder of the invocation
# releases — not when the creator exits, and not while any sibling still runs. So
# every acquire records ITS OWN pid against the container ids it holds, dead holders
# are pruned on every access (a killed worker neither wedges the pair up forever nor
# tears it down under a survivor), and the release that leaves no live holder owns
# the teardown.
#
# The registry lives under the invocation's state dir (see ``invocation_state_dir``)
# because that is exactly its visibility requirement: only the workers of ONE
# invocation can reach that invocation's state file, so only they can ever hold
# references to its pair. Pid recycling can only ever KEEP a container (fail-safe);
# ``SWEEP_AGE_LIMIT`` bounds that leak.

_HOLDERS_FILENAME = "holders.json"
_HOLDERS_LOCK_FILENAME = "holders.lock"


@contextmanager
def _holder_registry(state_dir: Path) -> Generator[Path, None, None]:
    """The invocation's registry file, under its own lock. Always taken INSIDE the
    pair lock where both are held, so the two never deadlock against each other."""
    from filelock import FileLock

    with FileLock(str(state_dir / _HOLDERS_LOCK_FILENAME)):
        yield state_dir / _HOLDERS_FILENAME


def _read_holders(path: Path) -> dict[str, list[int]]:
    """``{container_id: [holder pid, ...]}`` with every dead holder — and every
    container left with none — dropped. A missing, empty or corrupt file reads as
    "nothing is held": the same self-healing tolerance the counters use."""
    raw: object = None
    with suppress(OSError, ValueError):
        raw = json.loads(path.read_text())
    if not isinstance(raw, dict):
        return {}
    holders: dict[str, list[int]] = {}
    for key, value in cast("dict[object, object]", raw).items():
        if not isinstance(value, list):
            continue
        live = [
            pid for pid in cast("list[object]", value) if isinstance(pid, int) and pid_alive(pid)
        ]
        if live:
            holders[str(key)] = live
    return holders


def claim_container_holders(
    container_ids: Iterable[str], *, state_dir: Path, pid: int | None = None
) -> bool:
    """Record one reference held by *pid* on each of *container_ids* in *state_dir*'s
    registry, and report whether a live holder ALREADY existed before this claim.

    One entry PER ACQUISITION, duplicates included: the published surface wraps the
    pair twice per worker (``pg_container`` and ``redis_container``), and each release
    must retire exactly one of them.

    The pre-existing answer and the claim are one locked step: a release cannot slip
    between "was anyone holding it" and "now I hold it" and leave the caller believing
    it joined a live crowd that vanished.
    """
    holder = os.getpid() if pid is None else pid
    ids = list(container_ids)
    with _holder_registry(state_dir) as path:
        holders = _read_holders(path)
        already_held = any(container_id in holders for container_id in ids)
        for container_id in ids:
            holders.setdefault(container_id, []).append(holder)
        _atomic_write_text(path, json.dumps(holders))
        return already_held


def release_container_holders(
    container_ids: Iterable[str], *, state_dir: Path, pid: int | None = None
) -> bool:
    """Retire one reference held by *pid* on each of *container_ids* in *state_dir*'s
    registry; return whether NO live holder remains on any of them — i.e. whether
    this release is the last one and the caller owns the teardown."""
    holder = os.getpid() if pid is None else pid
    ids = list(container_ids)
    with _holder_registry(state_dir) as path:
        holders = _read_holders(path)
        for container_id in ids:
            remaining = holders.get(container_id)
            if remaining and holder in remaining:
                remaining.remove(holder)
            if not remaining:
                holders.pop(container_id, None)
        _atomic_write_text(path, json.dumps(holders))
        return not any(container_id in holders for container_id in ids)


def _recorded_pair_is(info_path: Path, info: SharedServices) -> bool:
    """Whether the state file still names *info*'s pair — the guard on unlinking it,
    since another worker's fresh start may already have replaced it."""
    with suppress(OSError, ValueError, TypeError):
        return SharedServices(**json.loads(info_path.read_text())) == info
    return False


def _log_pair_event(
    event: str,
    info: SharedServices,
    *,
    state_dir: Path | None = None,
    reason: str | None = None,
) -> None:
    """One ``[TaskQ]``-prefixed structured line per shared-pair decision point
    (``pair-started`` / ``pair-reused`` / ``pair-fresh-started`` /
    ``pair-stopped``), matching the clock-divergence diagnostic's style — this
    module has no structlog setup. Keys are stable per event; the 12-char
    container-id prefixes are enough to cross-reference ``docker ps`` without
    dumping full ids, and ``state_dir`` identifies the invocation that owns the
    pair when debugging who started/reused/stopped it."""
    parts = [
        f"event={event}",
        f"pg={info.pg_container_id[:12]}",
        f"redis={info.redis_container_id[:12]}",
    ]
    if reason is not None:
        parts.append(f"reason={reason}")
    if state_dir is not None:
        parts.append(f"state_dir={state_dir}")
    print("[TaskQ] " + " ".join(parts))


@contextmanager
def shared_service_pair(state_dir: Path) -> Generator[SharedServices, None, None]:
    """Acquire the shared Postgres + Dragonfly pair for one session-fixture lifetime.

    The first caller to take the lock starts the pair and publishes connection info
    into *state_dir* (the per-invocation pytest tmpdir — :func:`invocation_state_dir`;
    every xdist worker of the invocation shares it, and no other invocation of any
    repo can find it); later callers — in any worker of the invocation — reuse it.
    Each acquire registers its own pid as a holder of the pair's containers and each
    release retires it; the pair is torn down by the release that leaves NO live
    holder. A recorded pair that no live process holds and whose labeled owners are
    all dead (every trace of the invocation that owned this state dir is gone) is
    removed and re-created rather than reused, and stale containers from crashed runs
    are swept before starting fresh ones.

    Every decision point logs one ``[TaskQ]`` line with stable keys
    (:func:`_log_pair_event`): ``pair-started`` (no prior state), ``pair-reused``,
    ``pair-fresh-started`` (a recorded pair was rejected — the reason says why), and
    ``pair-stopped`` on the last release.

    Both session fixtures that share the pair (``pg_container`` in ``tests/conftest.py``
    and ``redis_container`` in :mod:`taskq.testing.fixtures`) wrap this context manager
    independently, so one worker registers two holder references; the pair is stopped
    exactly once, by the release that retires the last live one.
    """
    from filelock import FileLock

    info_path = state_dir / _INFO_FILENAME
    redis_db_path = state_dir / _REDIS_DB_FILENAME
    lock = FileLock(str(state_dir / _LOCK_FILENAME))

    with lock:
        info: SharedServices | None = None
        fresh_start_reason: str | None = None
        if info_path.exists():
            try:
                candidate = SharedServices(**json.loads(info_path.read_text()))
            except (ValueError, TypeError):
                candidate = None  # corrupt/torn state file: start fresh below
            if candidate is None:
                fresh_start_reason = "corrupt-state-file"
            else:
                recorded = (candidate.pg_container_id, candidate.redis_container_id)
                already_held = claim_container_holders(recorded, state_dir=state_dir)
                if not (container_running(recorded[0]) and container_running(recorded[1])):
                    release_container_holders(recorded, state_dir=state_dir)
                    fresh_start_reason = "recorded-containers-not-running"
                elif already_held or services_have_live_owner(candidate):
                    # Either liveness signal keeps the pair: a live HOLDER (a sibling
                    # worker of this invocation is using it right now, whether or not
                    # it started it) or a live labeled OWNER (the registry file was
                    # wiped, or the pair was started between the two writes). Only
                    # when both say "nobody" is the pair a dead run's leftover and
                    # safe to replace. The claim above is this acquire's own reference.
                    info = candidate
                else:
                    release_container_holders(recorded, state_dir=state_dir)
                    stop_shared_services(candidate)
                    info_path.unlink(missing_ok=True)
                    fresh_start_reason = "recorded-pair-unheld"
        if info is None:
            info = start_shared_services()
            _atomic_write_text(info_path, json.dumps(asdict(info)))
            # The Dragonfly's logical DBs die with the container: a fresh pair means a
            # fresh DB space, so the counter starts from zero again (a mid-invocation
            # fresh start would otherwise march toward exhaustion on dead DBs).
            _atomic_write_text(redis_db_path, "0")
            _log_pair_event(
                "pair-fresh-started" if fresh_start_reason else "pair-started",
                info,
                state_dir=state_dir,
                reason=fresh_start_reason,
            )
            claim_container_holders(
                (info.pg_container_id, info.redis_container_id), state_dir=state_dir
            )
        else:
            _log_pair_event("pair-reused", info)

    try:
        yield info
    finally:
        with lock:
            if release_container_holders(
                (info.pg_container_id, info.redis_container_id), state_dir=state_dir
            ):
                stop_shared_services(info)
                # Only OUR pair's record: a fresh start by another worker may have
                # already replaced the file, and unlinking that would strand its pair.
                if _recorded_pair_is(info_path, info):
                    info_path.unlink(missing_ok=True)
                _log_pair_event("pair-stopped", info)


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
