"""Unit tests for the e2e tier's image-hygiene rules (no Docker needed).

The Docker I/O wrappers in :mod:`tests.e2e._image_hygiene` are exercised by
every e2e session (the sweeps run at ``e2e_network`` setup, the teardown by
``e2e_worker_image``); these tests pin the pure keep/remove decisions those
wrappers rely on — the same split ``tests/test_shared_containers.py`` applies
to ``taskq.testing._shared_containers``.

This module requests none of the shared infra fixtures, so the autouse
``clean_e2e_state`` guard in the e2e conftest early-yields without booting
any container.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from taskq.testing._shared_containers import SWEEP_AGE_LIMIT

from ._image_hygiene import (
    WORKER_IMAGE_PREFIX,
    should_sweep_stale_wheel_cache_entry,
    should_sweep_stale_wheel_scratch_dir,
    should_sweep_stale_worker_image,
    worker_image_target,
)

pytestmark = [pytest.mark.e2e]

# Fixed clock: the decisions are pure functions of (repository/name, created,
# now), so a frozen now makes every age assertion exact.
_NOW = datetime(2026, 9, 3, 12, 0, 0, tzinfo=UTC)
_FRESH = timedelta(seconds=30)
_PAST_LIMIT = SWEEP_AGE_LIMIT + timedelta(hours=1)


def _dead_pid() -> int:
    """A pid that provably does not exist: spawn a trivial child and reap it
    (mirrors ``tests/test_shared_containers.py``'s helper)."""
    proc = subprocess.Popen(  # Why: fixed argv, project interpreter; spawns a provably-dead pid for liveness tests.
        [sys.executable, "-c", "pass"]
    )
    proc.wait(timeout=10)
    return proc.pid


def test_worker_image_target_keeps_the_sweep_prefix() -> None:
    """The pid-owned repository name MUST keep starting with the shared
    ``taskq-e2e-worker`` prefix: that prefix is what makes every worker
    CONTAINER a sweep candidate in ``taskq.testing._shared_containers``
    (``_SWEEP_IMAGE_PREFIXES`` matches ``Config.Image`` by prefix), and what
    lets ``make clean-e2e`` recognize this tier's images."""
    target = worker_image_target(os.getpid())
    assert target.startswith(WORKER_IMAGE_PREFIX)
    assert target.endswith(f"-r{os.getpid()}")
    # The name is a valid Docker repository: no tag colon, no path separators.
    assert "/" not in target and ":" not in target


def test_pid_owned_image_sweeps_iff_owner_dead() -> None:
    """Dead owner pid → remove; live owner pid → keep. The pid suffix is the
    whole owner identity (the image twin of the stale-network sweep rule)."""
    dead = worker_image_target(_dead_pid())
    live = worker_image_target(os.getpid())
    assert should_sweep_stale_worker_image(repository=dead, created=_NOW - _FRESH, now=_NOW) is True
    assert (
        should_sweep_stale_worker_image(repository=live, created=_NOW - _FRESH, now=_NOW) is False
    )


def test_pid_owned_image_age_backstop_beats_recycled_pid() -> None:
    """Over the 24 h age limit a pid-owned image is swept even if its pid
    number now belongs to an unrelated live process — pid recycling must not
    shield a stray forever (same backstop the container sweep carries)."""
    live_owner = worker_image_target(os.getpid())
    assert (
        should_sweep_stale_worker_image(repository=live_owner, created=_NOW - _PAST_LIMIT, now=_NOW)
        is True
    )


def test_legacy_and_foreign_repositories_are_never_auto_swept() -> None:
    """The automatic sweep never touches legacy exact-name images (a
    concurrent checkout running pre-ownership code can still build into that
    name — ``make clean-e2e`` owns them) or any unrecognized name shape."""
    repositories = [
        WORKER_IMAGE_PREFIX,  # legacy, pre-pid-ownership
        f"{WORKER_IMAGE_PREFIX}-r12x",  # suffix is not a pid
        f"{WORKER_IMAGE_PREFIX}-worker",  # other shape, still prefixed
        "postgres",
        "",
    ]
    for repository in repositories:
        assert (
            should_sweep_stale_worker_image(
                repository=repository, created=_NOW - _PAST_LIMIT, now=_NOW
            )
            is False
        ), repository


def test_wheel_scratch_dir_sweeps_iff_owner_dead_or_old() -> None:
    """``dist-e2e-<pid>`` scratch dirs follow the pid-owner rule, with the
    age backstop; the content-addressed wheel cache dir and any other name
    are not scratch dirs."""
    dead = f"dist-e2e-{_dead_pid()}"
    live = f"dist-e2e-{os.getpid()}"
    assert should_sweep_stale_wheel_scratch_dir(name=dead, created=_NOW - _FRESH, now=_NOW) is True
    assert should_sweep_stale_wheel_scratch_dir(name=live, created=_NOW - _FRESH, now=_NOW) is False
    assert (
        should_sweep_stale_wheel_scratch_dir(name=live, created=_NOW - _PAST_LIMIT, now=_NOW)
        is True
    )
    for name in ("dist-e2e-wheels", "dist-e2e-abc", "dist-e2e-", "dist", ""):
        assert (
            should_sweep_stale_wheel_scratch_dir(name=name, created=_NOW - _PAST_LIMIT, now=_NOW)
            is False
        ), name


def test_cached_wheel_entry_is_age_swept_only() -> None:
    """Cached wheel entries carry no owner identity — identical content is
    legitimately reusable by any live session — so only the age backstop
    applies."""
    assert should_sweep_stale_wheel_cache_entry(created=_NOW - _FRESH, now=_NOW) is False
    assert should_sweep_stale_wheel_cache_entry(created=_NOW - SWEEP_AGE_LIMIT, now=_NOW) is False
    assert should_sweep_stale_wheel_cache_entry(created=_NOW - _PAST_LIMIT, now=_NOW) is True
