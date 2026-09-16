"""Red-team: workgroup TOML validation gaps beyond the known provider opt-out.

Two config shapes pass ``WorkgroupConfig.from_toml`` validation today
that cannot produce a working child:

1. **A worker name whose health socket path cannot bind anywhere.**
   ``_spawn_child`` builds the child's mandatory health socket as
   ``/tmp/taskq_health_{name}_{instance_id}.sock``
   (src/taskq/worker/workgroup.py:520) — a 60-char fixed overhead (18
   prefix + 1 underscore + 36 UUID + 5 ".sock") plus the name. The
   validator's own stated intent is the socket path limit
   (src/taskq/worker/workgroup.py:314-315: "name must be <= 64 chars
   (socket path limit)"), but 64 + 60 = 124 chars exceeds every
   supported platform's ``sockaddr_un.sun_path`` (108 on Linux, 104 on
   macOS/BSD) — ``asyncio.start_unix_server`` raises ``OSError: AF_UNIX
   path too long`` for anything over the platform budget. The worker
   binds the health socket UNCONDITIONALLY at startup
   (src/taskq/worker/_bootstrap.py:1239-1240 ``await health_server.start(deps)``
   — no health.enabled gate on the bind), so a long-named child dies at
   spawn, and the supervisor restart-loops it against the burst budget
   until ``gave_up`` — a permanently dead worker blessed by validation.
   Any name ≥ 49 chars yields a path ≥ 109 chars, unbindable on BOTH
   major platforms.

2. **``queues = []``** — a worker that consumes nothing. The supervisor
   validator rejects every other certainly-broken shape (poll_interval
   <= 0, missing names, duplicate names, an empty workers list), and the
   project's own unconsumed-queue warning work (pinned at
   tests/test_worker_unconsumed_queue_warning.py:244-249) classifies a
   worker consuming no queues as "a certain misconfiguration rather than
   the ambiguous fleet case" — the worker child warns, but the workgroup
   config that CAUSED it says nothing. An operator-authored explicit
   empty list is always a mistake, and the workgroup is the one place
   that can reject it before a useless process exists.

Both tests assert the DESIRED observable (ValueError at load) and are
RED today. A short-name / explicit-single-queue GREEN control guards
against over-rejection. Pure in-memory — no PG, no subprocess.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from taskq.worker.workgroup import WorkgroupConfig

_ACTORS = "myapp.actors:registry"

# 18 (prefix "/tmp/taskq_health_") + 1 (underscore) + 36 (UUID str) + 5
# (".sock") = 60 chars of fixed overhead around the worker name.
_SOCKET_FIXED_OVERHEAD = 60
_LINUX_SUN_PATH = 108  # sockaddr_un.sun_path on Linux (incl. NUL)
_MACOS_SUN_PATH = 104  # sockaddr_un.sun_path on macOS/BSD (incl. NUL)

# Two distinct boundaries, both derived from the constants:
_BINDABLE_ON_ALL_PLATFORMS = min(_LINUX_SUN_PATH, _MACOS_SUN_PATH) - 1 - _SOCKET_FIXED_OVERHEAD
"""Longest name whose health socket path still binds everywhere (macOS
is the tighter budget: 103 usable path chars - 60 overhead = 43)."""

_UNBINDABLE_EVERYWHERE_MIN = _LINUX_SUN_PATH - _SOCKET_FIXED_OVERHEAD + 1
"""Shortest name whose health socket path binds NOWHERE (Linux is the
most generous budget: 107 usable path chars, so a path of 109+ fails on
Linux AND macOS) = 49."""


def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "wg.toml"
    path.write_text(body)
    return path


def test_rejects_worker_name_that_overflows_the_health_socket_path(tmp_path: Path) -> None:
    """RED: a name whose health socket path exceeds every supported
    platform's sun_path must be rejected at load — not accepted by
    validation and then killed at child spawn by ``OSError: AF_UNIX
    path too long``.

    Current behavior: src/taskq/worker/workgroup.py:314-315 accepts any
    name <= 64 chars, but 64 + 60 fixed chars = 124 > 108 (Linux) and
    > 104 (macOS) — the validator's own 'socket path limit' intent is
    off by ~17-20 chars, and the child binds the socket unconditionally
    (src/taskq/worker/_bootstrap.py:1240), so the worker dies at spawn
    and restart-loops into burst give-up."""
    name = "w" * _UNBINDABLE_EVERYWHERE_MIN  # path = 109 chars: unbindable on Linux AND macOS
    cfg = _write_config(
        tmp_path,
        f"""
actors = "{_ACTORS}"

[[workers]]
name = "{name}"
queues = ["default"]
""",
    )
    with pytest.raises(ValueError, match="socket") as excinfo:
        WorkgroupConfig.from_toml(cfg)
    assert name[:8] in str(excinfo.value) or "worker" in str(excinfo.value), (
        "the rejection must name the offending worker so the operator can find it"
    )


def test_rejects_empty_queues_list(tmp_path: Path) -> None:
    """RED: ``queues = []`` produces a child that dispatches nothing — the
    project's own classification of that state is 'a certain
    misconfiguration' (tests/test_worker_unconsumed_queue_warning.py::
    test_empty_queues_with_actors_warns_worker_consumes_nothing). The
    workgroup config is operator-authored and its validator exists to
    reject exactly this class of mistake before a useless process
    spawns; today ``_require_list_str`` (src/taskq/worker/workgroup.py:
    267-272) accepts the empty list and the child silently consumes no
    queue."""
    cfg = _write_config(
        tmp_path,
        f"""
actors = "{_ACTORS}"

[[workers]]
name = "silent"
queues = []
""",
    )
    with pytest.raises(ValueError, match="queue"):
        WorkgroupConfig.from_toml(cfg)


def test_accepts_short_name_and_explicit_queues(tmp_path: Path) -> None:
    """GREEN control against over-rejection: a short name (socket path well
    inside every platform budget) with an explicit queue list loads
    cleanly — the RED tests above must be fixed by tightening the two
    specific gaps, not by refusing configs wholesale."""
    cfg = _write_config(
        tmp_path,
        f"""
actors = "{_ACTORS}"

[[workers]]
name = "api"
queues = ["default", "batch"]
""",
    )
    config = WorkgroupConfig.from_toml(cfg)
    assert config.workers[0].name == "api"
    assert config.workers[0].queues == ["default", "batch"]


def test_name_boundaries_are_calculated_honestly() -> None:
    """GREEN arithmetic pin: both boundaries the tests use are derived from
    the real constants, not magic numbers — a fix that corrects the
    validator's bound must keep this arithmetic honest (fixed overhead
    60; macOS sun_path 104 is the everywhere-bindable budget → name 43;
    Linux sun_path 108 is the most generous → name 49 already fails
    everywhere)."""
    health_path_prefix = "/tmp/taskq_health_"  # noqa: S108  # Why: must byte-match the production prefix at src/taskq/worker/workgroup.py:520 — the literal under test.
    uuid_len = 36
    suffix_len = len("_") + len(".sock")
    fixed = len(health_path_prefix) + uuid_len + suffix_len
    assert fixed == _SOCKET_FIXED_OVERHEAD, (
        f"the fixed overhead around the worker name in the health socket path "
        f"(src/taskq/worker/workgroup.py:520) is {fixed} chars, not "
        f"{_SOCKET_FIXED_OVERHEAD} — update _SOCKET_FIXED_OVERHEAD and the "
        "boundary arithmetic together"
    )
    assert _BINDABLE_ON_ALL_PLATFORMS == 43, (
        "macOS sun_path is 104 bytes including the NUL terminator, so the "
        "usable path is 103 chars and the everywhere-bindable name budget "
        "is 103 - 60 = 43"
    )
    assert _UNBINDABLE_EVERYWHERE_MIN == 49, (
        "Linux sun_path is 108 bytes including the NUL terminator, so a path "
        "of 108+ chars fails even there; name 49 -> path 109 fails on every "
        "supported platform"
    )
