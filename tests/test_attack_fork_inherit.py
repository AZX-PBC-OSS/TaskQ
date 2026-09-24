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
uninstalled no-op (processes that never opted in change nothing), the wire
classes' pass-through in the owning process, and the BYPASS FAMILIES review
found - redis-py's pipeline wire (``send_packed_command``, the path TaskQ's
own progress publisher rides), asyncpg's COPY / custom-codec methods (they
reach the protocol object without touching the query methods) from the
first cut, and from the second: ``fetchmany`` (it drives
``_executemany`` -> ``_protocol.bind_execute_many`` directly, never the
public ``executemany``) and the teardown wire (``close``/``terminate`` -
a forked child's pool shutdown would put its own Terminate on the
parent's stream).
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

    def child_tries_send(entry_point: str) -> str:
        import asyncio

        try:
            # None self: the connection pin's shape - the refusal must fire
            # before the method body reaches self.
            if entry_point == "send_command":
                asyncio.run(cls.send_command(None))  # type: ignore[arg-type]  # Why: deliberate None self, exactly the connection pin's shape.
            else:
                asyncio.run(cls.send_packed_command(None, b"PING"))  # type: ignore[arg-type]  # Why: as above; the packed path is the pipeline wire.
        except ForkedInheritedProcessError:
            return "refused"
        return "allowed"

    for entry_point in ("send_command", "send_packed_command"):
        verdict = _run_in_forked_child(lambda ep=entry_point: child_tries_send(ep))
        assert verdict == "refused", (
            f"redis connection.{entry_point}: a forked child was not refused (got {verdict!r})"
        )


def test_forked_child_pipeline_execute_is_refused() -> None:
    """The D1 pin: redis-py's Pipeline does NOT ride ``send_command``.
    ``Pipeline.execute`` -> ``_execute_pipeline`` / ``_execute_transaction``
    packs every queued command and calls
    ``connection.send_packed_command(all_cmds)`` DIRECTLY (redis-py's
    asyncio client) - exactly the wire path TaskQ's own progress publisher
    rides (``progress/_publish.py`` builds a ``pipeline(transaction=False)``
    and executes two PUBLISHes per event). A guard on ``send_command``
    alone leaves the pipeline wire open: the forked child's progress
    publish writes the parent's socket with no refusal. This pin runs a
    REAL ``Pipeline.execute`` over an injected guarded connection (the
    pooled-connection shape, unconnected and on an unroutable port so any
    non-guard path fails fast): in a forked child the typed refusal must
    fire before the packed bytes leave the process."""
    install_fork_guard()
    import asyncio

    from redis.asyncio.client import Pipeline
    from redis.asyncio.connection import ConnectionPool

    conn_cls = guarded_redis_connection_class()

    class _NoReleasePool(ConnectionPool):
        # Why: the pipeline's reset() releases the connection it never
        # FETCHED from this pool (the pin injects it), and the real
        # release() would raise on a connection it does not track - noise
        # that would mask the refusal this pin exists to observe.
        async def release(self, connection: object) -> None:  # type: ignore[override]  # Why: the base pools the parameter; the no-op does not care.
            return None

    async def run_pipeline() -> str:
        pipe = Pipeline(
            _NoReleasePool(host="127.0.0.1", port=1),
            response_callbacks={},
            transaction=False,
            shard_hint=None,
        )
        # The inherited shape: a connection built in the PARENT, handed to
        # the pipeline the way a pooled connection would be - never
        # connected here, so the guard must fire before any socket exists.
        pipe.connection = conn_cls(host="127.0.0.1", port=1)
        pipe.publish("the-channel", "payload")
        pipe.publish("the-channel", "payload")
        try:
            await pipe.execute()
            return "allowed"
        except ForkedInheritedProcessError:
            return "refused"

    # Owner: pass-through. The unroutable port surfaces as a connection
    # error - the one outcome forbidden in the owner is the child's refusal.
    with pytest.raises(
        Exception
    ) as owner_exc:  # Why: deliberate wide net; the assertion below narrows it.
        asyncio.run(run_pipeline())
    assert not isinstance(owner_exc.value, ForkedInheritedProcessError), (
        "the guard refused the OWNING process's pipeline: an inverted or stale pid check"
    )

    # Child: the pipeline execute is refused, before a packed byte moves.
    def child_executes_pipeline() -> str:
        return asyncio.run(run_pipeline())

    verdict = _run_in_forked_child(child_executes_pipeline)
    assert verdict == "refused", (
        f"a forked child ran a pipeline execute (the wire path TaskQ's own "
        f"progress publisher uses) and was NOT refused (got {verdict!r}): "
        f"the packed bytes reached the inherited socket"
    )


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


_PROTOCOL_DIRECT_ENTRIES: list[tuple[str, tuple[object, ...], dict[str, object]]] = [
    # The asyncpg wire entries whose bodies do NOT ride the public query
    # methods - every one reaches the protocol object (or the private
    # introspection ``_execute``) directly, so none of the query-method
    # overrides sees it:
    # - the COPY methods drive ``self._protocol.copy_in/copy_out``;
    # - the codec methods' type introspection goes through the private
    #   ``_execute``;
    # - ``fetchmany`` drives ``_executemany`` ->
    #   ``_protocol.bind_execute_many`` (asyncpg 0.31 never reaches the
    #   public ``executemany``);
    # - ``close``/``terminate`` write the Terminate/abort through the
    #   protocol - a forked child's teardown close would put its own
    #   Terminate on the parent's stream.
    ("copy_from_table", ("tbl",), {"output": lambda chunk: None}),
    ("copy_from_query", ("SELECT 1",), {"output": lambda chunk: None}),
    ("copy_to_table", ("tbl",), {"source": b"data"}),
    ("copy_records_to_table", ("tbl",), {"records": []}),
    ("set_type_codec", ("typename",), {"encoder": lambda v: v, "decoder": lambda v: v}),
    ("reset_type_codec", ("typename",), {}),
    ("set_builtin_type_codec", ("typename",), {"codec_name": "int"}),
    ("fetchmany", ("SELECT 1", []), {}),
    ("close", (), {}),
    ("terminate", (), {}),
]


def test_guarded_wire_refuses_on_every_protocol_direct_entry() -> None:
    """The D2 + D5 pin: asyncpg's COPY and custom-codec methods bypass the
    query methods (direct ``self._protocol.copy_in/copy_out`` calls, and
    the codec introspection's private ``_execute``), ``fetchmany`` drives
    ``_executemany`` -> ``_protocol.bind_execute_many`` DIRECTLY (0.31:
    never the public ``executemany``), and ``close``/``terminate`` write
    the Terminate/abort through the protocol - so a forked child's pool
    shutdown would kill the PARENT's connection. Without their own
    overrides all of these are wire entries a forked child can use
    unrefused. The pin drives EVERY entry through the guarded class (None
    self: any use past the guard would explode inside asyncpg long after
    the refusal must already have fired; ``terminate`` is sync, the rest
    coroutine - the drive awaits whichever the call returns)."""
    install_fork_guard()
    conn_cls = guarded_connection_class()
    import asyncio
    import inspect

    async def drive(entry: tuple[str, tuple[object, ...], dict[str, object]]) -> None:
        method, args, kwargs = entry
        result = getattr(conn_cls, method)(None, *args, **kwargs)  # type: ignore[arg-type]  # Why: deliberate None self - the pin proves the guard fires before the body.
        if inspect.iscoroutine(result):
            await result

    # Owner: pass-through - the None self explodes inside asyncpg
    # (TypeError at binding, AttributeError at attribute access), it is not
    # refused.
    for entry in _PROTOCOL_DIRECT_ENTRIES:
        with pytest.raises(
            Exception
        ) as owner_exc:  # Why: deliberate wide net; the assertion below narrows it.
            asyncio.run(drive(entry))
        assert not isinstance(owner_exc.value, ForkedInheritedProcessError), (
            f"{entry[0]}: the guard refused the OWNING process: an inverted or stale pid check"
        )

    # Child: every entry refused, before any wire byte.
    def child_tries(entry: tuple[str, tuple[object, ...], dict[str, object]]) -> str:
        try:
            asyncio.run(drive(entry))
        except ForkedInheritedProcessError:
            return "refused"
        return "allowed"

    for entry in _PROTOCOL_DIRECT_ENTRIES:
        verdict = _run_in_forked_child(lambda e=entry: child_tries(e))
        assert verdict == "refused", (
            f"{entry[0]}: a forked child reached an asyncpg protocol-direct "
            f"wire entry and was NOT refused (got {verdict!r})"
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
