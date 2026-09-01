"""Unit tests for the shared-container machinery in ``taskq.testing._shared_containers``.

That module exists to keep ONE Postgres + ONE Dragonfly across all xdist workers (a
``-n 4`` run previously booted four of each, and the contention was the measured cause
of heavy PG tests tripping pytest-timeout intermittently). Its keep/remove and
refcount decisions are pure file/OS state, so the whole contract is testable without
Docker: the Docker I/O wrappers around them are exercised by every integration run
and by the container census.

The sweep contract pinned here is ported from the proven sibling implementation
(cennan): a liveness-blind sweep once force-removed a CONCURRENTLY running pytest
session's containers on a shared Docker daemon (~98x ConnectionRefusedError), so the
label rules below are load-bearing, not hygiene.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from taskq.testing import _shared_containers as sc

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch

_NOW = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)
_PG_IMAGE = "postgres:18-alpine"
_DRAGONFLY_IMAGE = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"


def _decide(
    *,
    image: str = _PG_IMAGE,
    name: str = "nostalgic_turing",
    labels: dict[str, str] | None = None,
    running: bool = True,
    created: datetime = _NOW - timedelta(hours=1),
    now: datetime = _NOW,
) -> bool:
    return sc.should_sweep_stale_container(
        image=image,
        name=name,
        labels=labels or {},
        running=running,
        created=created,
        now=now,
    )


def _dead_pid() -> int:
    """A pid that provably does not exist: spawn a trivial child and reap it."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "pass"]
    )  # Why: fixed argv, project interpreter; spawns a provably-dead pid for liveness tests.
    proc.wait(timeout=10)
    return proc.pid


# ── Ownership labels ────────────────────────────────────────────────────────


def test_creator_labels_record_this_process_and_its_parent() -> None:
    """Two pids because the creating xdist WORKER can exit before sibling workers
    finish; the parent (the xdist controller, or the shell under -n0) outlives
    every worker, so its pid keeps proving the owning run is alive."""
    labels = sc.creator_labels()
    assert set(labels) == {sc.CREATOR_PID_LABEL, sc.CONTROLLER_PID_LABEL}
    assert int(labels[sc.CREATOR_PID_LABEL]) == os.getpid()
    assert int(labels[sc.CONTROLLER_PID_LABEL]) == os.getppid()


def test_owner_pid_label_re_matches_any_sibling_repo_shape() -> None:
    """Cross-repo courtesy on a shared daemon: cennan/warden label with the same
    ``<project>.test.(creator|controller)-pid`` key shape, and every repo's sweep
    must honor ANY of them so one checkout's sweep never kills another's live run."""
    assert sc.OWNER_PID_LABEL_RE.match("taskq.test.creator-pid")
    assert sc.OWNER_PID_LABEL_RE.match("cennan.test.controller-pid")
    assert sc.OWNER_PID_LABEL_RE.match("warden.test.creator-pid")
    assert sc.OWNER_PID_LABEL_RE.match("myapp.test.controller-pid")
    assert not sc.OWNER_PID_LABEL_RE.match("org.testcontainers.session-id")
    assert not sc.OWNER_PID_LABEL_RE.match("taskq.test.creator-pid-extra")
    assert not sc.OWNER_PID_LABEL_RE.match("com.example.taskq.creator-pid")


def test_labeled_pids_collects_parseable_pids_across_all_owner_labels() -> None:
    """All parseable pid values across ours AND sibling repos' labels; an
    unparseable value cannot have come from a real labeling site (all write
    ``str(pid)``), protects no live run, and is dropped."""
    labels = {
        sc.CREATOR_PID_LABEL: "123",
        "warden.test.controller-pid": "456",
        "unrelated": "789",
        "cennan.test.creator-pid": "not-a-pid",
    }
    assert sc.labeled_pids(labels) == [123, 456]


def test_pid_alive_self_true_and_reaped_child_false() -> None:
    assert sc.pid_alive(os.getpid()) is True
    assert sc.pid_alive(_dead_pid()) is False


# ── Sweep decision ──────────────────────────────────────────────────────────


def test_a_container_whose_creator_process_is_alive_is_kept() -> None:
    labels = {sc.CREATOR_PID_LABEL: str(os.getpid()), sc.CONTROLLER_PID_LABEL: str(os.getppid())}
    assert _decide(labels=labels) is False


def test_a_container_with_a_dead_creator_but_a_live_controller_is_kept() -> None:
    """The xdist case the second label exists for: the creating worker can exit
    before sibling workers finish, so a dead creator pid alone must not condemn
    containers still in use."""
    labels = {sc.CREATOR_PID_LABEL: str(_dead_pid()), sc.CONTROLLER_PID_LABEL: str(os.getpid())}
    assert _decide(labels=labels) is False


def test_a_container_with_a_live_creator_but_a_dead_controller_is_kept() -> None:
    labels = {sc.CREATOR_PID_LABEL: str(os.getpid()), sc.CONTROLLER_PID_LABEL: str(_dead_pid())}
    assert _decide(labels=labels) is False


def test_a_container_is_swept_once_every_labeled_owner_is_dead() -> None:
    labels = {
        sc.CREATOR_PID_LABEL: str(_dead_pid()),
        sc.CONTROLLER_PID_LABEL: str(_dead_pid()),
    }
    assert _decide(labels=labels) is True


def test_an_ancient_labeled_container_is_swept_even_with_a_live_owner_pid() -> None:
    """Age backstop: over days a dead run's pid can be recycled by an unrelated
    live process — liveness alone would shield the leftover forever."""
    labels = {sc.CREATOR_PID_LABEL: str(os.getpid())}
    assert _decide(labels=labels, created=_NOW - sc.SWEEP_AGE_LIMIT - timedelta(seconds=1)) is True


def test_a_labeled_container_exactly_at_the_age_limit_with_a_live_owner_is_kept() -> None:
    labels = {sc.CREATOR_PID_LABEL: str(os.getpid())}
    assert _decide(labels=labels, created=_NOW - sc.SWEEP_AGE_LIMIT) is False


def test_an_unparseable_creator_pid_label_is_treated_as_stale() -> None:
    """Real labeling sites only ever write ``str(pid)``, so a non-integer value
    protects nobody and is swept."""
    assert _decide(labels={sc.CREATOR_PID_LABEL: "not-a-pid"}) is True


def test_a_running_unlabeled_container_is_kept() -> None:
    """A live unlabeled container may belong to a live run of pre-label code, so
    it is left alone; only the 24h backstop clears it."""
    assert _decide() is False


def test_an_exited_unlabeled_container_is_swept() -> None:
    assert _decide(running=False) is True


def test_an_ancient_unlabeled_container_is_swept_even_when_running() -> None:
    assert _decide(running=True, created=_NOW - sc.SWEEP_AGE_LIMIT - timedelta(seconds=1)) is True


def test_a_running_unlabeled_container_exactly_at_the_age_limit_is_kept() -> None:
    assert _decide(running=True, created=_NOW - sc.SWEEP_AGE_LIMIT) is False


def test_a_foreign_repo_label_with_a_live_pid_is_honored() -> None:
    assert _decide(labels={"warden.test.creator-pid": str(os.getpid())}) is False


def test_a_foreign_repo_label_whose_pids_are_all_dead_is_swept() -> None:
    assert (
        _decide(
            labels={
                "warden.test.creator-pid": str(_dead_pid()),
                "myapp.test.controller-pid": str(_dead_pid()),
            }
        )
        is True
    )


def test_the_compose_dev_stack_is_never_swept() -> None:
    """The docker-compose dev stack pins ``container_name: taskq-*``; those
    containers are never ours to remove, whatever their image or state."""
    for name in ("taskq-postgres", "taskq-redis", "taskq-admin"):
        assert (
            _decide(
                name=name,
                labels={sc.CREATOR_PID_LABEL: str(_dead_pid())},
                running=False,
            )
            is False
        )


def test_images_outside_the_sweep_prefixes_are_ignored() -> None:
    """Only TaskQ's own test images are managed. Notably the compose dev stack's
    ``postgres:18.4`` does not match the exact test-image prefix
    ``postgres:18-alpine``, so the sweep cannot touch it even by image."""
    assert _decide(image="redis:8.6.3") is False
    assert _decide(image="hello-world") is False
    assert _decide(image="postgres:18.4", running=False) is False


def test_taskq_test_images_match_the_sweep_prefixes() -> None:
    """Both the shared pair and the disposable chaos containers run these exact
    images, so a crashed run's leftovers are sweepable once their owners die."""
    assert _decide(image=_PG_IMAGE, running=False) is True
    assert _decide(image=_DRAGONFLY_IMAGE, running=False) is True


# ── shared_service_pair: lock + refcount + reuse-or-start ───────────────────


def _fake_services() -> sc.SharedServices:
    return sc.SharedServices(
        pg_dsn="postgresql://taskq:taskq@127.0.0.1:5555/taskq",
        pg_container_id="fake-pg-id",
        redis_host="127.0.0.1",
        redis_port=6666,
        redis_container_id="fake-redis-id",
    )


@pytest.fixture
def pair_fakes(monkeypatch: MonkeyPatch) -> dict[str, list[object]]:
    """Replace every Docker-touching helper the pair context manager calls, and
    record the calls so assertions can inspect the lifecycle decisions."""
    calls: dict[str, list[object]] = {"start": [], "stop": [], "running": [], "live_owner": []}

    def fake_start() -> sc.SharedServices:
        calls["start"].append(None)
        return _fake_services()

    def fake_stop(info: sc.SharedServices) -> None:
        calls["stop"].append(info)

    def fake_running(container_id: str) -> bool:
        calls["running"].append(container_id)
        return True

    def fake_live_owner(info: sc.SharedServices) -> bool:
        calls["live_owner"].append(info)
        return True

    monkeypatch.setattr(sc, "start_shared_services", fake_start)
    monkeypatch.setattr(sc, "stop_shared_services", fake_stop)
    monkeypatch.setattr(sc, "container_running", fake_running)
    monkeypatch.setattr(sc, "services_have_live_owner", fake_live_owner)
    return calls


def test_first_acquire_starts_the_pair_and_writes_state(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    with sc.shared_service_pair(tmp_path) as services:
        assert services == _fake_services()
        assert (tmp_path / "taskq-test-services.json").exists()
        assert (tmp_path / "taskq-test-services.json").read_text() == json.dumps(
            {
                "pg_dsn": "postgresql://taskq:taskq@127.0.0.1:5555/taskq",
                "pg_container_id": "fake-pg-id",
                "redis_host": "127.0.0.1",
                "redis_port": 6666,
                "redis_container_id": "fake-redis-id",
            }
        )
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
        assert (tmp_path / "taskq-test-services.redis-db").read_text() == "0"
    assert len(pair_fakes["start"]) == 1


def test_later_acquires_reuse_the_running_pair_without_starting(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    """While any reference is held, later acquire/release cycles (workers joining
    and finishing at different times) reuse the running pair; only the LAST
    release stops it. Two full sequential lifecycles with nothing held in
    between would rightly tear down and restart."""
    with sc.shared_service_pair(tmp_path):
        with sc.shared_service_pair(tmp_path):
            pass
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
        assert pair_fakes["stop"] == []
        with sc.shared_service_pair(tmp_path):
            pass
        assert len(pair_fakes["start"]) == 1
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
        assert pair_fakes["stop"] == []
    assert len(pair_fakes["start"]) == 1
    assert (tmp_path / "taskq-test-services.count").read_text() == "0"


def test_last_release_stops_the_pair_and_unlinks_state(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    with sc.shared_service_pair(tmp_path):
        first = (tmp_path / "taskq-test-services.count").read_text()
        with sc.shared_service_pair(tmp_path):
            second = (tmp_path / "taskq-test-services.count").read_text()
        # Inner release: one reference still held, pair must stay up.
        assert (first, second) == ("1", "2")
        assert pair_fakes["stop"] == []
        assert (tmp_path / "taskq-test-services.json").exists()
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
    # Outer release: refcount zero → pair stopped, state unlinked.
    assert (tmp_path / "taskq-test-services.count").read_text() == "0"
    assert pair_fakes["stop"] == [_fake_services()]
    assert not (tmp_path / "taskq-test-services.json").exists()


def test_a_recorded_pair_whose_owners_are_all_dead_is_removed_not_reused(
    tmp_path: Path, monkeypatch: MonkeyPatch, pair_fakes: dict[str, list[object]]
) -> None:
    """The crashed-run case: the state files outlive their run (the refcount never
    returned to zero) with both labeled pids dead, and blindly reusing the pair
    would hand a NEW run containers a concurrent run's sweep would then remove
    mid-session. The pair must be stopped and re-created instead. The fresh
    start also resets the refcount — the dead run's leaked reference is garbage
    nobody will ever release, so this run becomes the sole live owner and tears
    the fresh pair down cleanly on exit."""
    leaked = _fake_services()
    (tmp_path / "taskq-test-services.json").write_text(json.dumps(asdict(leaked)))
    (tmp_path / "taskq-test-services.count").write_text("1")

    monkeypatch.setattr(sc, "services_have_live_owner", lambda info: False)
    with sc.shared_service_pair(tmp_path) as services:
        assert services == _fake_services()
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
    assert pair_fakes["stop"] == [leaked, _fake_services()]
    assert len(pair_fakes["start"]) == 1
    assert (tmp_path / "taskq-test-services.count").read_text() == "0"
    assert not (tmp_path / "taskq-test-services.json").exists()


def test_a_crash_torn_count_file_is_tolerated(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    """A crash between truncate and write can leave the count file empty; a
    corrupt count must never break suite startup. Zero is the self-healing
    value: this joiner becomes the sole reference, so teardown still removes
    the pair (worst case a lingered pair the next run's sweep clears)."""
    with sc.shared_service_pair(tmp_path):
        pass
    (tmp_path / "taskq-test-services.count").write_text("")
    with sc.shared_service_pair(tmp_path):
        assert (tmp_path / "taskq-test-services.count").read_text() == "1"
    assert (tmp_path / "taskq-test-services.count").read_text() == "0"


def test_a_corrupt_info_file_starts_fresh(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    (tmp_path / "taskq-test-services.json").write_text("{not json")
    with sc.shared_service_pair(tmp_path) as services:
        assert services == _fake_services()
    assert len(pair_fakes["start"]) == 1


# ── Redis logical-DB allocation ─────────────────────────────────────────────


def test_redis_db_allocation_is_unique_and_monotonic(tmp_path: Path) -> None:
    """Every consumer (module or test function) across EVERY xdist worker draws
    from one file-backed counter, so two workers can never collide on the same
    logical DB in the shared Dragonfly — a collision would let one consumer's
    FLUSHDB wipe another's mid-run state."""
    assert [sc.next_redis_logical_db(tmp_path) for _ in range(3)] == [1, 2, 3]


def test_pair_start_resets_the_redis_db_counter(tmp_path: Path) -> None:
    """The DBs live in the container: a fresh pair means a fresh DB space, so
    starting one resets the counter (serial ``-n0`` runs share the state dir
    across runs and would otherwise march toward exhaustion)."""
    assert sc.next_redis_logical_db(tmp_path) == 1
    (tmp_path / "taskq-test-services.redis-db").write_text("0")
    assert sc.next_redis_logical_db(tmp_path) == 1


def test_redis_db_allocation_exhaustion_raises_loudly(tmp_path: Path) -> None:
    """Never wrap around: sharing a DB between consumers would let one
    consumer's FLUSHDB wipe another's mid-run state."""
    (tmp_path / "taskq-test-services.redis-db").write_text(str(sc.REDIS_DB_POOL_SIZE - 1))
    with pytest.raises(RuntimeError, match="exhausted"):
        sc.next_redis_logical_db(tmp_path)


def test_redis_db_counter_survives_a_torn_write(tmp_path: Path) -> None:
    (tmp_path / "taskq-test-services.redis-db").write_text("")
    assert sc.next_redis_logical_db(tmp_path) == 1


# ── Session-fixture wrapping (the integration shape, without Docker) ───────


def test_two_session_fixtures_over_one_pair_teardown_once(
    tmp_path: Path, pair_fakes: dict[str, list[object]]
) -> None:
    """The published surface wraps the pair TWICE per worker (pg_container and
    redis_container each hold a reference); the pair must be stopped exactly
    once, when the last reference of the last worker releases."""

    def pg_like(shim_services: sc.SharedServices) -> Iterator[str]:
        with sc.shared_service_pair(tmp_path):
            yield shim_services.pg_dsn

    def redis_like(shim_services: sc.SharedServices) -> Iterator[int]:
        with sc.shared_service_pair(tmp_path):
            yield shim_services.redis_port

    pg_gen = pg_like(_fake_services())
    redis_gen = redis_like(_fake_services())
    assert next(pg_gen).startswith("postgresql://")
    assert next(redis_gen) == 6666
    assert (tmp_path / "taskq-test-services.count").read_text() == "2"
    # Simulate the two session fixtures finalizing in arbitrary order.
    next(pg_gen, None)
    assert pair_fakes["stop"] == []
    next(redis_gen, None)
    assert pair_fakes["stop"] == [_fake_services()]
    assert not (tmp_path / "taskq-test-services.json").exists()
