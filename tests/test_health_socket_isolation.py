"""Regression tests: tests must never share the default health socket path.

CI flake: ``WorkerSettings.health_socket_path`` defaults to the shared
production path ``/tmp/taskq_health.sock`` and ``_main`` starts a real
:class:`HealthServer`. Under pytest-xdist, two workers inside ``_main``
concurrently then race on one filesystem path — the loser raises
``EADDRINUSE`` (a TOCTOU window in ``asyncio.create_unix_server``'s
stale-file removal), or worse, silently steals the socket from the live
winner. The conftest shim redirects any ``HealthServer.start`` that targets
the shared default to a unique module-scoped path; these tests pin that
guarantee so the flake cannot return.
"""

import os
from types import SimpleNamespace
from typing import cast

from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import HealthServer


def _default_settings() -> WorkerSettings:
    """Settings with the health socket left at its shared production default."""
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})


def _deps(settings: WorkerSettings) -> WorkerDeps:
    """Duck-typed deps: HealthServer.start/stop only read ``deps.settings``."""
    return cast(WorkerDeps, SimpleNamespace(settings=settings))


def test_unique_health_sock_path_unique_per_call() -> None:
    from tests.conftest import unique_health_sock_path

    assert unique_health_sock_path("mymod") != unique_health_sock_path("mymod")


def test_unique_health_sock_path_module_scoped_and_short() -> None:
    from tests.conftest import unique_health_sock_path

    path = unique_health_sock_path("mymod")
    assert "mymod" in path
    assert str(os.getpid()) in path
    # macOS AF_UNIX sun_path limit is 104 chars — keep well under it.
    assert len(path) < 80


async def test_concurrent_health_servers_with_default_settings_do_not_conflict() -> None:
    """Two in-flight worker bootstraps (xdist siblings) must not share one socket.

    Both servers are started with the shared default path in settings —
    exactly what ``_main``-driving tests did before the fix. The shim must
    redirect each to a distinct path, and the first server's socket must
    still be its own (not stolen) after the second starts.
    """
    default_path = _default_settings().health_socket_path

    server_a = HealthServer()
    await server_a.start(_deps(_default_settings()))
    try:
        server_b = HealthServer()
        await server_b.start(_deps(_default_settings()))
        try:
            path_a = server_a._socket_path  # pyright: ignore[reportPrivateUsage]  # Why: test seam — asserting the bound path, which start() does not expose publicly.
            path_b = server_b._socket_path  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
            assert path_a is not None and path_b is not None
            assert path_a != default_path
            assert path_b != default_path
            assert path_a != path_b
            # A still owns its path — B did not steal it by rebinding.
            inode_a = server_a._socket_inode  # pyright: ignore[reportPrivateUsage]  # Why: test seam — ownership is tracked via the bound inode.
            assert os.stat(path_a).st_ino == inode_a
        finally:
            await server_b.stop()
    finally:
        await server_a.stop()


async def test_reused_settings_object_across_servers_still_gets_distinct_paths() -> None:
    """One settings object reused across two starts (e.g. a worker-restart
    test) must still yield two distinct sockets — the first redirect must
    not 'consume' the isolation."""
    settings = _default_settings()

    server_a = HealthServer()
    await server_a.start(_deps(settings))
    try:
        server_b = HealthServer()
        await server_b.start(_deps(settings))
        try:
            path_a = server_a._socket_path  # pyright: ignore[reportPrivateUsage]  # Why: test seam — asserting the bound path, which start() does not expose publicly.
            path_b = server_b._socket_path  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
            assert path_a is not None and path_b is not None
            assert path_a != path_b
            inode_a = server_a._socket_inode  # pyright: ignore[reportPrivateUsage]  # Why: test seam — ownership is tracked via the bound inode.
            assert os.stat(path_a).st_ino == inode_a
        finally:
            await server_b.stop()
    finally:
        await server_a.stop()


async def test_explicit_non_default_path_is_bound_verbatim() -> None:
    """The shim must not rewrite a path the test chose explicitly."""
    from tests.conftest import unique_health_sock_path

    explicit = unique_health_sock_path("explicit")
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": explicit,
        }
    )

    server = HealthServer()
    await server.start(_deps(settings))
    try:
        assert server._socket_path == explicit  # pyright: ignore[reportPrivateUsage]  # Why: test seam — asserting the bound path, which start() does not expose publicly.
        assert settings.health_socket_path == explicit
    finally:
        await server.stop()
