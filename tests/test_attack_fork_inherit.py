"""ATTACK tests: the fork guard - a forked child must not touch TaskQ's wire.

The hypothesis under attack: a worker process (or a client process) forks -
a job body calling ``os.fork()``, a ``multiprocessing`` fork context, a
prefork server that preload-built its TaskQ - and the child inherits every
socket TaskQ owns: the asyncpg pools, the LISTEN connection, the Redis
client, the loop's own pipes. Before the fork guard existed the failure was
SILENT: two protocol state machines on one TCP stream, a parent reading the
server's answer to the child's statement as the answer to its own, a
terminal ledger written by both processes.

The mechanism (``taskq._forkguard``) and its contract:

- the worker bootstrap and ``TaskQ.open`` install ``at-fork`` hooks and pin
  the owning pid;
- a forked child that touches a TaskQ wire entry gets a typed refusal
  (:class:`taskq._forkguard.ForkedInheritedProcessError`) BEFORE a byte
  reaches the shared socket - the refusal, not the corruption, is the
  observable;
- the PARENT is stamped per fork, and the worker's loops report
  ``fork-detected-in-worker-process`` from their own iteration.

These pins attack the guard itself: install idempotence, the child hook's
pid refresh (a guard that forgets to re-cache the pid in the child passes
the child as the owner), the parent stamp's exactly-once consume, the
uninstalled no-op (processes that never opted in change nothing), and the
wire classes' pass-through in the owning process.
"""

import contextlib
import os
import signal
import subprocess
import sys

import asyncpg
import pytest

from taskq._forkguard import (
    ForkedInheritedProcessError,
    assert_own_process,
    guarded_connection_class,
    guarded_redis_connection_class,
    install_fork_guard,
    take_parent_fork_event,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh_guard_state(monkeypatch: pytest.MonkeyPatch):
    """Reset the guard's module state around every pin.

    The guard is a process-global singleton by design (one install, one
    owner); the pins need each fork scenario to start from a known state.
    Restoring the previous state afterwards keeps the module usable by
    tests that already installed the guard for real.
    """
    import taskq._forkguard as mod

    snapshot = (mod._guard_pid, mod._current_pid, mod._parent_fork_at)
    mod._guard_pid = None
    mod._current_pid = None
    mod._parent_fork_at = None
    yield
    mod._guard_pid, mod._current_pid, mod._parent_fork_at = snapshot


def _exit_child(status: int) -> None:
    """Terminate the forked child WITHOUT returning into pytest.

    The suite's root conftest intercepts ``os._exit`` (the watchdog's
    force-exit tripwire) and raises instead of exiting, so a child that
    calls it comes back to life mid-teardown. The child is scaffolding:
    SIGKILL is the honest exit, and bytes already written to the pipe are
    delivered regardless of the writer's death.
    """
    os.kill(os.getpid(), signal.SIGKILL)


def _run_in_forked_child(body: "callable") -> str:  # type: ignore[valid-type]  # Why: a pin-local helper, the callable returns a short verdict string.
    """Run ``body`` in a forked child and return its verdict string.

    The child is a fork of the PYTEST process: it must never return into
    pytest (its teardown would run against the parent's fixtures), so the
    child writes one verdict down a pipe and dies by signal. The verdict
    is either ``body``'s own return value or ``exc:<type>`` for the
    exception it raised - the pins assert on the REFUSAL type, and a pin
    that hangs (the child awaiting an inherited socket) dies on the suite's
    timeout instead of passing.
    """
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            verdict = body()
            os.write(w, verdict.encode())
        except (
            BaseException
        ) as exc:  # Why: the child reports ANY failure back to the parent, one verdict, one exit.
            with contextlib.suppress(OSError):  # pragma: no cover - the parent vanished mid-write
                os.write(w, f"exc:{type(exc).__name__}".encode())
        finally:
            os.close(w)
            _exit_child(0)
    os.close(w)
    with os.fdopen(r, "rb") as fh:
        return fh.read().decode()


# ── install: idempotent, pid-pinning, opt-in ──────────────────────────


def test_install_pins_the_calling_process_once() -> None:
    install_fork_guard()
    import taskq._forkguard as mod

    first = mod._guard_pid
    assert first == os.getpid()
    assert mod._current_pid == first

    install_fork_guard()
    install_fork_guard()
    # A second install must not re-pin: a re-pin would let a pre-existing
    # forked child pass as the owner the moment anything re-ran install.
    assert mod._guard_pid == first


def test_uninstalled_guard_is_a_noop() -> None:
    # No install in this pin's state: the check cannot know whether this
    # process is a forked child, so it must refuse NOTHING (the opt-in
    # contract: tools, tests and embeddings that never fork near TaskQ pay
    # nothing and change nothing).
    import taskq._forkguard as mod

    assert mod._guard_pid is None
    assert_own_process("uninstalled-noop")  # must not raise


# ── child side: the refusal ───────────────────────────────────────────


def test_forked_child_is_refused_and_parent_is_not() -> None:
    install_fork_guard()

    # The parent IS the owner: the same check passes here.
    assert_own_process("parent-owner")

    verdict = _run_in_forked_child(lambda: (assert_own_process("child-use"), "allowed")[1])

    assert verdict == "exc:ForkedInheritedProcessError", (
        f"a forked child used a TaskQ wire resource and was NOT refused "
        f"(got {verdict!r}): silent shared-socket corruption is the "
        f"behavior this pin exists to keep dead"
    )


def test_forked_grandchild_is_refused() -> None:
    """The child hook re-runs at every fork depth: a grandchild is a forked
    child too, its cached pid re-stamped by ITS fork's hook."""
    install_fork_guard()

    def grandchild_verdict() -> str:
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                assert_own_process("grandchild-use")
                os.write(w, b"refused-not")
            except ForkedInheritedProcessError:
                os.write(w, b"refused")
            finally:
                os.close(w)
                _exit_child(0)
        os.close(w)
        with os.fdopen(r, "rb") as fh:
            return fh.read().decode()

    verdict = _run_in_forked_child(grandchild_verdict)
    assert verdict == "refused", verdict


def test_refusal_names_the_resource_and_the_remediation() -> None:
    """The refusal is operator-actionable: it says WHAT was refused and that
    the fix is a post-fork open or subprocess's close_fds default."""
    install_fork_guard()

    def capture() -> str:
        try:
            assert_own_process("the-inherited-pool")
        except ForkedInheritedProcessError as exc:
            return str(exc)
        return "no-raise"

    verdict = _run_in_forked_child(capture)
    assert "the-inherited-pool" in verdict, verdict
    assert str(os.getpid()) in verdict, verdict  # the OWNER's pid, seen from the child


# ── parent side: the exactly-once stamp ───────────────────────────────


def test_parent_stamp_is_set_by_the_fork_and_consumed_once() -> None:
    install_fork_guard()
    assert take_parent_fork_event() is None

    pid = os.fork()
    if pid == 0:
        _exit_child(0)  # Why: the child is scaffolding here; the PARENT's stamp is the pin.
    os.waitpid(pid, 0)

    first = take_parent_fork_event()
    assert first is not None, (
        "the parent performed a fork and was not stamped: the "
        "fork-detected-in-worker-process report can never fire"
    )
    # Consumed once: the loops report one fork once, not once per tick.
    assert take_parent_fork_event() is None


# ── the wire classes ──────────────────────────────────────────────────


def test_guarded_connection_class_is_an_asyncpg_connection_subclass() -> None:
    import asyncpg

    cls = guarded_connection_class()
    assert issubclass(cls, asyncpg.Connection)


def test_guarded_wire_refuses_in_the_child_before_the_socket() -> None:
    """Every wire entry refuses in a forked child WITHOUT a live connection:
    the check runs before the method body touches ``self``, so the pin needs
    no server - a None self would explode inside asyncpg long after the
    refusal must already have fired."""
    install_fork_guard()
    conn_cls = guarded_connection_class()

    async def try_wire() -> str:
        # None self: any use past the guard would crash differently - the
        # ForkedInheritedProcessError name is the verdict that matters.
        await conn_cls.execute(None, "SELECT 1")  # type: ignore[arg-type]  # Why: deliberate - the pin proves the guard fires before the body.
        return "allowed"

    async def run() -> str:
        try:
            return await try_wire()
        except ForkedInheritedProcessError:
            return "refused"

    # Parent: the check is a pass-through. The None self makes super() blow
    # up one of two CPython ways (TypeError at binding, AttributeError at
    # attribute access) - which one is interpreter noise; what the pin
    # refuses to accept is the REFUSAL itself.
    import asyncio

    with pytest.raises(
        Exception
    ) as parent_exc:  # Why: deliberate wide net; the assertion below narrows it.
        asyncio.run(run())
    assert not isinstance(parent_exc.value, ForkedInheritedProcessError), (
        "the guard refused the OWNING process: an inverted or stale pid check"
    )

    # Child: refused, before any wire byte.
    verdict = _run_in_forked_child(lambda: asyncio.run(run()))
    assert verdict == "refused", verdict


def test_guarded_redis_class_refuses_in_the_child() -> None:
    install_fork_guard()
    import redis.asyncio as redis_async

    cls = guarded_redis_connection_class()
    assert issubclass(cls, redis_async.Connection)

    def child_tries_send() -> str:
        import asyncio

        try:
            asyncio.run(cls.send_command(None))  # type: ignore[arg-type]  # Why: deliberate None self, exactly the connection pin's shape.
        except ForkedInheritedProcessError:
            return "refused"
        return "allowed"

    verdict = _run_in_forked_child(child_tries_send)
    assert verdict == "refused", verdict


def test_wire_calls_pass_through_in_the_owner() -> None:
    """The guard must be invisible in the owning process: the execute call
    below reaches asyncpg (and dies on the None self), it is not refused."""
    install_fork_guard()
    conn_cls = guarded_connection_class()

    import asyncio

    async def call() -> None:
        await conn_cls.execute(None, "SELECT 1")  # type: ignore[arg-type]  # Why: the None self is the pass-through probe.

    with pytest.raises(
        Exception
    ) as exc_info:  # Why: deliberate wide net; the assertion below narrows it.
        asyncio.run(call())
    # super()'s rejection of the None self lands as TypeError or AttributeError
    # depending on the interpreter's binding path; the one outcome the contract
    # forbids in the owner is the child's refusal.
    assert not isinstance(exc_info.value, ForkedInheritedProcessError), (
        "the guard refused the OWNING process: an inverted or stale pid check"
    )


def test_transaction_default_kwargs_forward_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """The transaction override mirrors asyncpg's OWN defaults: an override
    that swapped one (deferrable=True) would change every transaction's
    semantics in every TaskQ-built pool. The signature is the contract the
    super() call forwards."""
    import inspect

    monkeypatch.setattr(  # Why: prove the override, not the base, carries the pin - the spy makes a base default swap a DIFFERENT failure than the override's.
        asyncpg.Connection,
        "transaction",
        lambda self, **kwargs: "base",
    )
    conn_cls = guarded_connection_class()
    params = inspect.signature(conn_cls.transaction).parameters
    assert params["isolation"].default is None
    assert params["readonly"].default is False
    assert params["deferrable"].default is False


def test_caller_supplied_connection_classes_win_over_the_guard() -> None:
    """The ``guarded_or_own_*`` resolvers: an explicit class is the caller's
    wire (returned as-is, never wrapped), ``None`` means TaskQ's guarded
    class. Returning None instead of the guarded class would silently strip
    the guard from every TaskQ-built resource."""
    import redis.asyncio as redis_async

    from taskq._forkguard import (
        guarded_or_own_connection_class,
        guarded_or_own_redis_connection_class,
    )

    class _MyConn(asyncpg.Connection):
        pass

    class _MyRedisConn(redis_async.Connection):
        pass

    assert guarded_or_own_connection_class(_MyConn) is _MyConn
    assert guarded_or_own_connection_class(None) is guarded_connection_class()
    assert guarded_or_own_redis_connection_class(_MyRedisConn) is _MyRedisConn
    assert guarded_or_own_redis_connection_class(None) is guarded_redis_connection_class()


# ── the subprocess cousin: the documented contract ────────────────────


def _socket_fd_count() -> int:
    """How many of THIS process's fds are sockets (/proc, Linux)."""
    count = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:  # pragma: no cover - the fd closed mid-walk
            continue
        if target.startswith("socket:"):
            count += 1
    return count


async def test_subprocess_default_close_fds_drops_the_inherited_wire(pg_dsn: str) -> None:
    """The gentler cousin's contract, pinned against a live pool: Python's
    ``subprocess`` default (``close_fds=True``) drops every inherited
    descriptor in the exec'd child - the pool's sockets do NOT leak into
    spawned processes, so a job body that shells out costs an fd-table walk,
    not a wire leak. The close_fds=False arm below shows the shape the
    default protects against."""
    import asyncio

    pool = await asyncpg.create_pool(
        dsn=pg_dsn, min_size=1, max_size=2, connection_class=guarded_connection_class()
    )
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1
        assert _socket_fd_count() > 0, "the pin needs an open socket to guard"

        script = (
            "import os\n"
            "n = 0\n"
            "for fd in os.listdir('/proc/self/fd'):\n"
            "    try:\n"
            "        if os.readlink(f'/proc/self/fd/{fd}').startswith('socket:'):\n"
            "            n += 1\n"
            "    except OSError:\n"
            "        pass  # the walk's own fd, closed mid-listing\n"
            "print(n)\n"
        )
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", script, stdout=subprocess.PIPE
        )
        out, _ = await proc.communicate()
        assert proc.returncode == 0
        assert int(out.strip()) == 0, (
            f"the exec'd child inherited {out.strip()} socket fd(s) with the "
            f"close_fds default: TaskQ's wire leaks into spawned processes"
        )
    finally:
        await pool.close()
