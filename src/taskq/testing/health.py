"""Health-socket path helpers for test suites.

Solves the EADDRINUSE-under-xdist problem with the worker health server:
``WorkerSettings.health_socket_path`` defaults to the shared production path
``/tmp/taskq_health.sock``, and ``worker._main`` starts a real
``HealthServer`` on it. Under pytest-xdist, two workers running ``_main``
in-process concurrently race on that one filesystem path — the loser gets
``EADDRINUSE`` (there is a TOCTOU window in ``create_unix_server``'s
stale-file removal), or silently steals the socket from the live winner.

Recommended consumer pattern — mint a unique path per test at settings
construction time::

    from taskq.testing.health import unique_health_sock_path
    from taskq.testing.settings import make_integration_settings

    settings = make_integration_settings(
        pg_dsn, HEALTH_SOCKET_PATH=unique_health_sock_path("my_test_module")
    )

Suites that build settings through the env cascade (``WorkerSettings.load()``)
instead of a factory should additionally redirect ``HealthServer.start`` away
from the shared default via an autouse monkeypatch — see
``tests/conftest.py::_isolate_health_server_socket`` in the TaskQ repo for the
reference implementation (it is repo-specific and deliberately not published).
"""

import os

from taskq._ids import new_base62

__all__ = ["unique_health_sock_path"]


def unique_health_sock_path(module: str) -> str:
    """Return a unique unix-socket path for one test's health server.

    ``/tmp/tq-<module>-<pid>-<token>.sock``: the pid scopes across xdist
    workers, the random token across tests within a worker (stateless — no
    shared counter, so uniqueness survives any pytest import mode), and the
    module label identifies the owner when debugging stale files. The short
    ``/tmp/tq-`` prefix keeps paths well under the 104-char AF_UNIX
    sun_path limit on macOS.

    Stale files are a resource-only leak: the per-run-unique token means a
    leftover path can never collide with a future run. ``HealthServer.stop``
    unlinks on normal completion; suites wanting a crash backstop can sweep
    ``/tmp/tq-*-<pid>-*.sock`` scoped to their own pid (see TaskQ's
    ``tests/conftest.py::_sweep_health_sock_files``).

    :param module: label embedded in the path. Must not contain path
        separators — ``/`` would point the socket at a nonexistent
        directory and surface as a confusing ENOENT at bind time.
    :raises ValueError: if *module* contains ``/`` or ``\\``.
    """
    if "/" in module or "\\" in module:
        raise ValueError(
            f"module label must not contain path separators, got {module!r} "
            "(it is embedded verbatim in the socket path)"
        )
    return f"/tmp/tq-{module}-{os.getpid()}-{new_base62()}.sock"  # noqa: S108  # Why: test-only socket files; /tmp is the shortest safe prefix.
