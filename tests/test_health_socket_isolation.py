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

import asyncio
import contextlib
import json
import os
from types import SimpleNamespace
from typing import cast

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.health import unique_health_sock_path
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import HealthServer
from taskq.worker.run import _main


def _default_settings() -> WorkerSettings:
    """Settings with the health socket left at its shared production default."""
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"})


def _deps(settings: WorkerSettings) -> WorkerDeps:
    """Duck-typed deps: HealthServer.start/stop only read ``deps.settings``."""
    return cast(WorkerDeps, SimpleNamespace(settings=settings))


def test_unique_health_sock_path_unique_per_call() -> None:
    from taskq.testing.health import unique_health_sock_path

    assert unique_health_sock_path("mymod") != unique_health_sock_path("mymod")


def test_unique_health_sock_path_module_scoped_and_short() -> None:
    from taskq.testing.health import unique_health_sock_path

    path = unique_health_sock_path("mymod")
    assert "mymod" in path
    assert str(os.getpid()) in path
    # macOS AF_UNIX sun_path limit is 104 chars — keep well under it.
    assert len(path) < 80


def test_unique_health_sock_path_rejects_path_separators() -> None:
    """A module label containing a path separator is rejected outright:
    embedded verbatim in the socket path, it would point at a nonexistent
    directory and surface as a confusing ENOENT at bind time."""
    from taskq.testing.health import unique_health_sock_path

    with pytest.raises(ValueError, match="path separator"):
        unique_health_sock_path("a/b")
    with pytest.raises(ValueError, match="path separator"):
        unique_health_sock_path("a\\b")


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
    from taskq.testing.health import unique_health_sock_path

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


# ── Production behaviour on a shared, operator-configured socket path ──
#
# The tests above pin the *test-suite* shim, which mints a distinct path per
# start(). Production has no such shim: TASKQ_HEALTH_SOCKET_PATH names one
# fixed path, and any deployment where two worker processes see the same
# filesystem at that path — a sidecar pair sharing an emptyDir/tmpfs mount, a
# workgroup whose children were given one path, a restart whose predecessor is
# still draining inside its termination grace period — puts two live servers
# on one path. The tests below drive that shape through the real code and
# assert what an operator's probe actually receives.


_PROBE_DEADLINE = 5.0


async def _probe(socket_path: str, endpoint: str) -> bytes:
    """Issue one probe request over the health socket and return the raw response.

    This is the operator's own vantage point: an orchestrator liveness or
    readiness probe opens the configured unix socket and reads the answer,
    with no knowledge of which process is on the other end. Every await is
    bounded so a silent socket fails the test instead of hanging it.
    """
    async with asyncio.timeout(_PROBE_DEADLINE):
        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            writer.write(f"GET {endpoint} HTTP/1.0\r\n\r\n".encode("ascii"))
            await writer.drain()
            return await reader.read()
        finally:
            writer.close()
            with contextlib.suppress(OSError, TimeoutError):
                await writer.wait_closed()


def _ready_body(response: bytes) -> dict[str, object]:
    """Parse the JSON body out of a raw probe response."""
    _head, _sep, body = response.partition(b"\r\n\r\n")
    parsed: dict[str, object] = json.loads(body)
    return parsed


async def test_live_health_socket_is_not_taken_over_by_a_later_server() -> None:
    """A second server started on a live server's socket path must not silently
    take the path over.

    Operationally this is the difference between a loud, diagnosable
    misconfiguration and a silent one. If the newcomer takes the path, the
    first worker keeps running and keeps claiming jobs while answering nothing
    on the socket its orchestrator probes; every probe an operator believes is
    checking that worker is answered by a different process entirely. A
    liveness probe then reports healthy for a worker nobody is checking, and a
    readiness probe reports the newcomer's state — including its shutdown
    phase — for a worker that is not shutting down.

    Either outcome is acceptable here: the second start refuses (the operator
    sees the conflict immediately), or both servers end up on distinct paths.
    What must not happen is the first server's bound socket being replaced
    while it is alive and serving.
    """
    shared_path = unique_health_sock_path("shared_live")
    settings_a = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": shared_path,
        }
    )
    settings_b = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": shared_path,
        }
    )

    server_a = HealthServer()
    await server_a.start(_deps(settings_a))
    server_b: HealthServer | None = None
    try:
        inode_a = os.stat(shared_path).st_ino

        server_b = HealthServer()
        try:
            await server_b.start(_deps(settings_b))
        except OSError:
            # A loud refusal is the acceptable outcome: the operator learns
            # about the collision at boot rather than through a probe that
            # quietly answers for the wrong process.
            server_b = None

        if server_b is not None:
            assert os.stat(shared_path).st_ino == inode_a, (
                "a second health server bound the socket path out from under a live "
                "server: the first worker keeps running and keeps claiming jobs while "
                "every orchestrator probe at its configured socket path is answered by "
                "the second process instead"
            )
    finally:
        if server_b is not None:
            await server_b.stop()
        await server_a.stop()


@pytest.mark.integration
async def test_probe_on_a_shared_socket_path_still_answers_for_the_first_worker(
    pg_dsn: str,
) -> None:
    """Two real workers sharing one configured socket path: the path must not
    start answering for the second worker while the first is alive.

    The discriminator is the maintenance-leader verdict, which the fleet
    assigns rather than the test: the first worker to boot wins the advisory
    lock and reports ``is_leader: true``; the second loses it and reports
    ``is_leader: false``. Both keep running. If a probe at the shared path
    flips from the leader's answer to the follower's, the operator's readiness
    check for the first worker is being served by the second — the first
    worker's real state (its shutdown phase, its in-flight job count, its
    leadership) has become unobservable at the address the orchestrator was
    told to use, while it continues to hold the leader lock and run work.
    """
    schema = f"thsi_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    shared_path = unique_health_sock_path("shared_fleet")

    def _worker_settings() -> WorkerSettings:
        return WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": schema,
                "queues": "default",
                "health_socket_path": shared_path,
            }
        )

    worker_a = asyncio.create_task(_main(_worker_settings()))
    worker_b: asyncio.Task[int] | None = None
    try:

        async def _leader_answering() -> bool:
            if worker_a.done():
                return False
            try:
                body = _ready_body(await _probe(shared_path, "/ready"))
            except (TimeoutError, OSError, ValueError):
                return False
            return body.get("is_leader") is True

        await wait_for_condition(
            _leader_answering,
            description="the first worker's health socket answering as the leader",
            timeout=30.0,
        )

        worker_b = asyncio.create_task(_main(_worker_settings()))

        async def _second_worker_registered() -> bool:
            probe = await asyncpg.connect(pg_dsn)
            try:
                count: int = await probe.fetchval(
                    f'SELECT count(*) FROM "{schema}".workers'  # noqa: S608  # Why: schema is a test-minted identifier, never user input.
                )
            finally:
                await probe.close()
            return count >= 2

        await wait_for_condition(
            _second_worker_registered,
            description="the second worker registering itself in the fleet",
            timeout=30.0,
        )

        assert not worker_a.done(), (
            "the first worker must still be running — it holds the leader lock "
            "and is the worker whose health the operator's probe is configured to read"
        )

        body = _ready_body(await _probe(shared_path, "/ready"))
        assert body.get("is_leader") is True, (
            "the probe at the configured health socket path stopped answering for the "
            "first worker once a second worker booted on the same path: it now reports "
            f"the second worker's state ({body!r}). The first worker is still alive and "
            "still holds the maintenance leader lock, but its liveness and readiness are "
            "no longer observable at the address its orchestrator probes — a probe "
            "failure will restart, or a probe success will keep routing to, a worker "
            "nobody is actually checking"
        )
    finally:
        for task in (worker_b, worker_a):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()


@pytest.mark.integration
async def test_second_worker_on_a_colliding_socket_path_still_boots_and_registers(
    pg_dsn: str,
) -> None:
    """A worker whose health socket collides with a live peer's must still start.

    ``HealthServer.start`` now refuses a colliding path loudly instead of
    silently stealing it (``_bind_unix_socket`` raises ``OSError`` on
    ``EADDRINUSE``) — that half of the fix is real and covered elsewhere in
    this file. But that refusal propagates out of ``HealthServer.start``
    completely unguarded: the call site in ``_bootstrap._main`` is

        if deps.settings.health_enabled:
            health_server = HealthServer()
            await health_server.start(deps)          # no try/except
            stack.push_async_callback(health_server.stop)

    with nothing catching the ``OSError``. A worker whose only misfortune is
    sharing a health-socket path with a still-live sibling therefore fails
    its entire boot — it never opens its Postgres pool for work, never
    registers in the fleet, never claims a job — over a problem confined to
    one diagnostic side-channel.

    This contradicts the project's own governing principle for worker boot
    (a worker that can do work must never fail to start; refusal is
    reserved for structural problems) and the issue's own scoped fix
    direction: on a health-socket collision the worker should log a loud
    warning and keep booting, not join the queue of things a boot refusal
    silently doubles as.

    The second worker here shares its healthy sibling's socket path on
    purpose. It must still show up in the fleet.
    """
    schema = f"thsi_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    shared_path = unique_health_sock_path("collide_boot")

    def _worker_settings() -> WorkerSettings:
        return WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": schema,
                "queues": "default",
                "health_socket_path": shared_path,
            }
        )

    worker_a = asyncio.create_task(_main(_worker_settings()))
    worker_b: asyncio.Task[int] | None = None
    try:

        async def _worker_a_answering() -> bool:
            if worker_a.done():
                return False
            try:
                await _probe(shared_path, "/ready")
            except (TimeoutError, OSError, ValueError):
                return False
            return True

        await wait_for_condition(
            _worker_a_answering,
            description="the first worker's health socket answering",
            timeout=30.0,
        )

        worker_b = asyncio.create_task(_main(_worker_settings()))

        async def _second_worker_registered() -> bool:
            probe = await asyncpg.connect(pg_dsn)
            try:
                count: int = await probe.fetchval(
                    f'SELECT count(*) FROM "{schema}".workers'  # noqa: S608  # Why: schema is a test-minted identifier, never user input.
                )
            finally:
                await probe.close()
            return count >= 2

        try:
            await wait_for_condition(
                _second_worker_registered,
                description="the second worker registering itself in the fleet "
                "despite its health socket colliding with the first worker's",
                timeout=15.0,
            )
        except TimeoutError:
            # It never registered. Find out why: did its boot task die?
            assert worker_b.done(), (
                "the second worker neither registered in the fleet nor is its "
                "boot task still running — it is stuck, not merely slow"
            )
            exc = worker_b.exception()
            assert exc is None, (
                "the second worker's boot crashed instead of continuing without a "
                f"working health listener, over a health-socket collision alone: {exc!r}. "
                "A worker that can do work must never fail to start; a health-socket "
                "collision is not a structural problem and must not abort boot."
            )
            raise

        assert not worker_b.done(), (
            "the second worker's boot task ended instead of running as a live worker"
        )
    finally:
        for task in (worker_b, worker_a):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
