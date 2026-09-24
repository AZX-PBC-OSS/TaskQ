"""The fork guard: a forked child must not touch TaskQ's inherited wire.

The attack shape this module exists for: a worker (or a client process)
forks while TaskQ's connections are live - a job body calling ``os.fork()``,
a ``multiprocessing`` context using its fork start method, an embedding
application that preload-instantiates :class:`taskq.TaskQ` in a master and
forks its workers (the gunicorn ``--preload`` shape), a library the job
body imports calling ``fork()`` under the covers. The child inherits every
file descriptor the parent holds: the asyncpg pool's sockets, the NOTIFY
LISTEN connection, the Redis client's socket, the event loop's self-pipe,
the OpenTelemetry exporters' pipes. Two processes now share one TCP stream:

- a child that writes anything to an inherited socket desynchronises the
  parent's wire protocol mid-query (the parent reads the server's answer
  to the CHILD's statement as the answer to its own);
- a child that merely holds a copy of a socket keeps the TCP connection
  alive after the parent dies: a ``kill -9`` of the parent leaves Postgres
  serving backends to a process that will never speak the protocol again;
- a child whose interpreter exits runs its own teardown (``atexit`` hooks,
  GC finalizers, exporter flushes) against inherited state its own threads
  never owned.

asyncpg is documented fork-unsafe (its connections are asyncio transports
bound to a loop and a protocol state machine); redis-py connections are
the same shape. Nothing in either library detects the fork. So before this
module existed the failure mode was silent corruption: the parent's next
query on the shared stream failed with a protocol error (or worse, matched
the wrong reply), the job's terminal write landed twice or not at all, and
no log line anywhere said why.

THE CONTRACT (fail loud at the wire, report loud in the parent):

1. **Child side - refuse the inherited wire.** TaskQ-built resources record
   the process that created them. Every wire entry point (each query method
   of a TaskQ-built asyncpg connection - the COPY and custom-codec methods
   included, they reach the protocol object directly and get their own
   overrides - each Redis command and each pipeline execute, each
   long-lived worker loop) checks the creating pid first. In a forked
   child the check raises :class:`ForkedInheritedProcessError` naming what
   was refused and what to do instead, BEFORE a single byte reaches the
   shared socket. The child that only ``exec``\\ s (``subprocess.Popen``,
   the default ``close_fds=True``) never runs Python again and is
   unaffected; a child that continues in Python gets a typed refusal
   instead of a corrupted stream.

   The negative scope, stated plainly: the guard rides only the resources
   TASKQ BUILDS (via its ``connection_class=``). A pool or connection the
   EMBEDDING APPLICATION built with plain ``asyncpg.create_pool()`` or
   ``redis.asyncio.from_url()`` BEFORE calling :meth:`taskq.TaskQ.open` -
   without passing TaskQ's guarded connection class - carries no guard, no
   matter that the same process also opened a TaskQ. The guard cannot see
   sockets it did not create; the operator docs carry the remediation
   (import the guard and pass the class, or open those resources after
   the fork).

2. **Parent side - the fork is reported, not silent.** The same ``at-fork``
   hook stamps the PARENT after every fork it performs. The worker's loops
   consume that stamp and emit ``fork-detected-in-worker-process``: a job
   body forked while this worker's connections were live, and the sockets
   the child still holds are the ones this pool will keep serving on. The
   parent keeps running - its connections were not written by the child
   unless the child also used them, and asyncpg's pool discards a
   protocol-dead connection and rebuilds, so a child that DID write recovers
   as a transient error on the in-flight job - but the log line is the
   operator's one honest signal that a fork happened and what it put at
   risk.

Why the pid check and not an ``os.register_at_fork(after_in_child=...)``
teardown of the resources: the child-side hook runs in a single-threaded,
allocation-hostile moment where closing (or, worse, protocol-terminating)
an inherited socket would itself write to the shared stream. Marking, not
closing, is the correct move; the child's refusal is instant and the
parent's sockets are never touched by the hook.

Why the guard does not kill the child: a prefork server's children are
supposed to keep running (that is what they are for). The contract is that
they run WITHOUT TaskQ's wire, and the refusal tells them so at the first
touch, per resource, rather than a blanket ``os._exit`` that would surprise
an embedding application that forked for reasons of its own.

The measured cost in the parent is one module-global identity comparison
per wire call (the child-side hook refreshes the cached pid, so no
``getpid()`` syscall ever rides the hot path).
"""

import os
import time
from collections.abc import Iterable, Sequence
from typing import Any

__all__ = [
    "ForkedInheritedProcessError",
    "assert_own_process",
    "guarded_connection_class",
    "guarded_or_own_connection_class",
    "guarded_or_own_redis_connection_class",
    "guarded_redis_connection_class",
    "install_fork_guard",
    "take_parent_fork_event",
]


class ForkedInheritedProcessError(RuntimeError):
    """Raised in a forked child that touches a TaskQ-owned wire resource.

    The process this code runs in was created by ``fork()`` from a process
    where TaskQ's fork guard is installed, so every TaskQ connection it
    inherited shares its TCP stream with the parent. Using one is silent
    wire corruption (two protocol state machines, one socket); the guard
    refuses instead. The fix is structural: a child that needs its own
    TaskQ opens one AFTER the fork (fresh sockets, its own event loop); a
    child that only runs another program should let ``subprocess``'s
    default ``close_fds=True`` drop the inherited descriptors.
    """


_guard_pid: int | None = None
"""The pid the fork guard was installed in. ``None`` until
:func:`install_fork_guard` runs, which makes every check a no-op: a process
that never opted in (tests, tools, embeddings that never fork near TaskQ)
pays nothing and changes nothing."""

_current_pid: int | None = None
"""The pid of the process LAST SEEN by an at-fork hook in this interpreter.

``install_fork_guard`` seeds it with the installing pid; the child-side
hook rewrites it when a fork lands here. Every check compares against this
cached value so the parent's hot path stays syscall-free. A fresh
interpreter (``multiprocessing`` spawn, ``exec``) starts ``None`` and is
safe by construction: the first install in THAT process re-seeds it."""

_parent_fork_at: float | None = None
"""Monotonic stamp of the most recent fork THIS (parent) process performed,
``None`` when none happened since the last :func:`take_parent_fork_event`."""

_guarded_connection_class: "type[Any] | None" = None
"""The process-wide guarded ``asyncpg.Connection`` subclass, built once."""

_guarded_redis_connection_class: "type[Any] | None" = None
"""The process-wide guarded ``redis.asyncio.Connection`` subclass, built
once."""


def _after_in_child() -> None:
    """``at-fork`` child hook: mark this process as a forked child.

    Runs in the CHILD immediately after ``fork`` returns, in the hostile
    moment: single thread, every lock in the process possibly held forever
    by a thread that no longer exists. It may therefore only touch module
    globals - no allocation beyond one int, no logging, no locks, no
    ``os.getpid()`` beyond the one read (getpid is async-signal-safe).

    The child that forks AGAIN re-runs this hook in the grandchild, so the
    cached pid stays truthful at any fork depth.
    """
    global _current_pid
    _current_pid = os.getpid()


def _after_in_parent() -> None:
    """``at-fork`` parent hook: stamp that this process forked.

    Runs in the PARENT right after the fork, where threads and locks are
    intact, so it may freely read the clock. It deliberately does NOT log
    here: the hook's stack is mid-syscall inside arbitrary user code (the
    logging machinery's own locks are fine in the parent, but the fork may
    be happening inside a log flush); the worker's loops consume the stamp
    and report from their own iteration, where the job's log context is
    bound and the loop is quiescent.
    """
    global _parent_fork_at
    _parent_fork_at = time.monotonic()


def install_fork_guard() -> None:
    """Install TaskQ's ``at-fork`` hooks and pin this process as the owner.

    Idempotent: the first call wins, later calls are no-ops (a second
    install must not re-register the hooks or re-pin the pid, which would
    let a pre-existing forked child pass as the owner). Callers: the worker
    bootstrap (before any pool opens, so every connection TaskQ builds is
    guarded) and :meth:`taskq.TaskQ.open` (the preload-then-fork embedding
    shape). On platforms without ``fork`` (Windows) the guard is a no-op:
    there is no fork to guard against.
    """
    global _guard_pid, _current_pid
    if _guard_pid is not None:
        return
    if not hasattr(os, "register_at_fork"):
        return
    _guard_pid = os.getpid()
    _current_pid = _guard_pid
    os.register_at_fork(after_in_child=_after_in_child, after_in_parent=_after_in_parent)


def assert_own_process(label: str) -> None:
    """Raise :class:`ForkedInheritedProcessError` in a forked child.

    *label* names the resource or loop at the check point, so the refusal
    says which inherited wire was refused, not just that something was.
    A no-op when the guard is not installed (``_guard_pid is None``) or
    when this interpreter IS the process the guard was installed in.
    """
    current = _current_pid
    if current is None or current is _guard_pid:
        return
    raise ForkedInheritedProcessError(
        f"{label}: this process (pid {current}) was forked from the process "
        f"that owns this TaskQ resource (guard pid {_guard_pid}), so the "
        f"connection's socket is shared with the parent. TaskQ's connections "
        f"(asyncpg pools, the NOTIFY LISTEN connection, the Redis client) are "
        f"not fork-safe: two processes writing one stream corrupts both. "
        f"Open a fresh TaskQ client/worker in the child AFTER the fork, or "
        f"run the child through subprocess.Popen (its default close_fds=True "
        f"drops the inherited descriptors)."
    )


def take_parent_fork_event() -> float | None:
    """Return and clear the stamp of the last fork this process performed.

    The worker's loops call this once per iteration: a return other than
    ``None`` means a ``fork()`` happened in THIS process since the last
    consume, and the caller reports it loud. Consuming clears the stamp so
    one fork is reported once, not once per loop tick forever.
    """
    global _parent_fork_at
    stamp = _parent_fork_at
    _parent_fork_at = None
    return stamp


def guarded_or_own_connection_class(
    connection_class: "type[Any] | None",
) -> "type[Any]":
    """The caller's connection class when it has one, TaskQ's guarded class
    when it does not.

    The seam :func:`taskq.auth.make_pg_pool_factory` and
    :func:`taskq.auth.make_dedicated_conn_factory` resolve through: a
    caller-supplied class is the caller's wire (the caller-owned doctrine,
    the guard does not silently wrap a class the caller chose), ``None``
    means TaskQ owns the connection and TaskQ's guard rides on it.
    """
    return connection_class if connection_class is not None else guarded_connection_class()


def guarded_connection_class() -> "type[Any]":
    """The :class:`asyncpg.Connection` subclass guarding every wire entry.

    Passed as ``connection_class=`` to every pool and dedicated connection
    TaskQ builds, so a forked child that reaches ANY query (a resumed
    worker loop finishing an inherited job's terminal write included) is
    refused before the socket is touched. Subclassing ``Connection`` is
    asyncpg's documented ``connection_class`` extension point; every
    override delegates to ``super()`` after the check, so behavior in the
    owning process is byte-identical.

    Deferred import and late subclass build: asyncpg is imported where the
    caller already imports it, and the class is built once per process.
    """
    import asyncpg

    global _guarded_connection_class
    if _guarded_connection_class is not None:
        return _guarded_connection_class

    _assert = assert_own_process
    _base = asyncpg.Connection

    class GuardedConnection(_base):  # type: ignore[misc, valid-type]  # Why: asyncpg types Connection as generic over Record; the runtime subclass needs no parameterisation.
        """Connection whose wire entry points refuse a forked child.

        The ``Any``-typed mirrors are deliberate: asyncpg's own stubs make
        the query methods generic over ``Record``, and a runtime subclass
        cannot re-declare that genericity. An ``Any`` mirror is assignment
        -compatible with the generic base (``Any`` is compatible in both
        directions), so the override is invisible to every caller's types.
        """

        async def execute(self, query: str, *args: Any, timeout: float | None = None) -> str:  # noqa: ASYNC109  # Why: mirrors asyncpg.Connection's own signature; the override must match it exactly, the kwarg is forwarded to super().
            _assert("asyncpg connection.execute")
            return await super().execute(query, *args, timeout=timeout)

        async def executemany(
            self,
            command: str,
            args: Iterable[Sequence[object]],
            *,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: same signature mirror as execute above.
        ) -> None:
            _assert("asyncpg connection.executemany")
            await super().executemany(command, args, timeout=timeout)

        async def fetch(
            self,
            query: str,
            *args: Any,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Connection's own signature; the override must match it exactly, the kwarg is forwarded to super().
            record_class: Any = None,
        ) -> Any:
            _assert("asyncpg connection.fetch")
            return await super().fetch(query, *args, timeout=timeout, record_class=record_class)

        async def fetchval(
            self,
            query: str,
            *args: Any,
            column: int = 0,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Connection's own signature; the override must match it exactly, the kwarg is forwarded to super().
        ) -> Any:
            _assert("asyncpg connection.fetchval")
            return await super().fetchval(query, *args, column=column, timeout=timeout)

        async def fetchrow(
            self,
            query: str,
            *args: Any,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Connection's own signature; the override must match it exactly, the kwarg is forwarded to super().
            record_class: Any = None,
        ) -> Any:
            _assert("asyncpg connection.fetchrow")
            return await super().fetchrow(query, *args, timeout=timeout, record_class=record_class)

        async def prepare(
            self,
            query: str,
            *,
            name: str | None = None,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Connection's own signature; the override must match it exactly, the kwarg is forwarded to super().
            record_class: Any = None,
        ) -> Any:
            _assert("asyncpg connection.prepare")
            return await super().prepare(
                query, name=name, timeout=timeout, record_class=record_class
            )

        def transaction(
            self, *, isolation: Any = None, readonly: bool = False, deferrable: bool = False
        ) -> Any:
            _assert("asyncpg connection.transaction")
            return super().transaction(
                isolation=isolation, readonly=readonly, deferrable=deferrable
            )

        def cursor(
            self,
            query: str,
            *args: Any,
            prefetch: int | None = None,
            timeout: float | None = None,
            record_class: Any = None,
        ) -> Any:
            _assert("asyncpg connection.cursor")
            return super().cursor(
                query, *args, prefetch=prefetch, timeout=timeout, record_class=record_class
            )

        async def add_listener(self, channel: str, callback: object) -> None:
            _assert("asyncpg connection.add_listener (the LISTEN wire)")
            await super().add_listener(channel, callback)  # type: ignore[arg-type]  # Why: asyncpg-stubs over-narrow the callback type; notify.py already carries the same suppression at its call sites.

        async def remove_listener(self, channel: str, callback: object) -> None:
            _assert("asyncpg connection.remove_listener")
            await super().remove_listener(channel, callback)  # type: ignore[arg-type]  # Why: same stub over-narrowing as add_listener.

        # The COPY and codec families: their bodies reach the protocol
        # object directly (self._protocol.copy_in/copy_out, or the private
        # _execute behind the type introspection) and never touch the
        # public query methods above, so without their own overrides they
        # are wire entries a forked child can use unrefused.

        async def copy_from_table(
            self,
            table_name: str,
            *,
            output: Any,
            columns: Any = None,
            schema_name: Any = None,
            timeout: Any = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's own signature; the override must match it exactly, the kwarg is forwarded to super().
            format: Any = None,  # Why: mirrors asyncpg.Connection.copy_from_table's own parameter name; the override must bind it exactly.
            oids: Any = None,
            delimiter: Any = None,
            null: Any = None,
            header: Any = None,
            quote: Any = None,
            escape: Any = None,
            force_quote: Any = None,
            encoding: Any = None,
        ) -> Any:
            _assert("asyncpg connection.copy_from_table (the COPY wire)")
            return await super().copy_from_table(
                table_name,
                output=output,
                columns=columns,
                schema_name=schema_name,
                timeout=timeout,
                format=format,
                oids=oids,
                delimiter=delimiter,
                null=null,
                header=header,
                quote=quote,
                escape=escape,
                force_quote=force_quote,
                encoding=encoding,
            )

        async def copy_from_query(
            self,
            query: str,
            *args: Any,
            output: Any,
            timeout: Any = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's own signature; the override must match it exactly, the kwarg is forwarded to super().
            format: Any = None,  # Why: mirrors asyncpg.Connection.copy_from_query's own parameter name.
            oids: Any = None,
            delimiter: Any = None,
            null: Any = None,
            header: Any = None,
            quote: Any = None,
            escape: Any = None,
            force_quote: Any = None,
            encoding: Any = None,
        ) -> Any:
            _assert("asyncpg connection.copy_from_query (the COPY wire)")
            return await super().copy_from_query(
                query,
                *args,
                output=output,
                timeout=timeout,
                format=format,
                oids=oids,
                delimiter=delimiter,
                null=null,
                header=header,
                quote=quote,
                escape=escape,
                force_quote=force_quote,
                encoding=encoding,
            )

        async def copy_to_table(
            self,
            table_name: str,
            *,
            source: Any,
            columns: Any = None,
            schema_name: Any = None,
            timeout: Any = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's own signature; the override must match it exactly, the kwarg is forwarded to super().
            format: Any = None,  # Why: mirrors asyncpg.Connection.copy_to_table's own parameter name.
            oids: Any = None,
            freeze: Any = None,
            delimiter: Any = None,
            null: Any = None,
            header: Any = None,
            quote: Any = None,
            escape: Any = None,
            force_quote: Any = None,
            force_not_null: Any = None,
            force_null: Any = None,
            encoding: Any = None,
            where: Any = None,
        ) -> Any:
            _assert("asyncpg connection.copy_to_table (the COPY wire)")
            return await super().copy_to_table(
                table_name,
                source=source,
                columns=columns,
                schema_name=schema_name,
                timeout=timeout,
                format=format,
                oids=oids,
                freeze=freeze,
                delimiter=delimiter,
                null=null,
                header=header,
                quote=quote,
                escape=escape,
                force_quote=force_quote,
                force_not_null=force_not_null,
                force_null=force_null,
                encoding=encoding,
                where=where,
            )

        async def copy_records_to_table(
            self,
            table_name: str,
            *,
            records: Any,
            columns: Any = None,
            schema_name: Any = None,
            timeout: Any = None,  # noqa: ASYNC109  # Why: mirrors asyncpg's own signature; the override must match it exactly, the kwarg is forwarded to super().
            where: Any = None,
        ) -> Any:
            _assert("asyncpg connection.copy_records_to_table (the COPY wire)")
            return await super().copy_records_to_table(
                table_name,
                records=records,
                columns=columns,
                schema_name=schema_name,
                timeout=timeout,
                where=where,
            )

        async def set_type_codec(
            self,
            typename: str,
            *,
            schema: Any = "public",
            encoder: Any,
            decoder: Any,
            format: Any = "text",  # Why: mirrors asyncpg.Connection.set_type_codec's own parameter name.
        ) -> None:
            _assert("asyncpg connection.set_type_codec (the codec introspection wire)")
            await super().set_type_codec(
                typename, schema=schema, encoder=encoder, decoder=decoder, format=format
            )

        async def reset_type_codec(self, typename: str, *, schema: Any = "public") -> None:
            _assert("asyncpg connection.reset_type_codec (the codec introspection wire)")
            await super().reset_type_codec(typename, schema=schema)

        async def set_builtin_type_codec(
            self,
            typename: str,
            *,
            schema: Any = "public",
            codec_name: Any,
            format: Any = None,  # Why: mirrors asyncpg.Connection.set_builtin_type_codec's own parameter name.
        ) -> None:
            _assert("asyncpg connection.set_builtin_type_codec (the codec introspection wire)")
            await super().set_builtin_type_codec(
                typename, schema=schema, codec_name=codec_name, format=format
            )

    _guarded_connection_class = GuardedConnection
    return GuardedConnection


def guarded_or_own_redis_connection_class(
    connection_class: "type[Any] | None",
) -> "type[Any]":
    """The redis twin of :func:`guarded_or_own_connection_class`: a
    caller-supplied class wins, ``None`` means TaskQ's guarded class."""
    return connection_class if connection_class is not None else guarded_redis_connection_class()


def guarded_redis_connection_class() -> "type[Any]":
    """The ``redis.asyncio.Connection`` subclass guarding command writes.

    Passed as ``connection_class=`` to every Redis client TaskQ builds (the
    worker's terminal-publish and rate-limit clients, the jobs client, the
    credential-provider factory). Two overrides because redis-py does NOT
    funnel everything through one method: plain commands leave through
    ``send_command``, but a pipeline's ``execute`` packs every queued
    command and writes them with ONE direct
    ``connection.send_packed_command(all_cmds)`` (``Pipeline
    ._execute_pipeline`` and ``._execute_transaction`` never call
    ``send_command``) - the shape TaskQ's own progress publisher rides
    (``progress/_publish.py`` builds a ``pipeline(transaction=False)`` and
    executes two PUBLISHes). Both entries are guarded, so a forked child's
    pipeline execute is refused before a byte reaches the inherited socket.
    """
    from redis.asyncio import Connection as RedisConnection

    global _guarded_redis_connection_class
    if _guarded_redis_connection_class is not None:
        return _guarded_redis_connection_class
    _assert = assert_own_process
    _base = RedisConnection

    class GuardedRedisConnection(_base):  # type: ignore[misc, valid-type]  # Why: redis-py types Connection without a parameteriser the runtime subclass needs.
        """Redis connection whose command writes refuse a forked child."""

        async def send_command(self, *args: object, **kwargs: object) -> None:
            _assert("redis connection.send_command")
            await super().send_command(*args, **kwargs)  # type: ignore[arg-type]  # Why: redis-py's own stubs type send_command loosely; the delegated call is the base's exact signature at runtime.

        async def send_packed_command(self, *args: object, **kwargs: object) -> None:
            _assert("redis connection.send_packed_command (the pipeline wire)")
            await super().send_packed_command(*args, **kwargs)  # type: ignore[arg-type]  # Why: redis-py's stubs type the packed path loosely; the delegate is the base's exact signature at runtime.

    _guarded_redis_connection_class = GuardedRedisConnection
    return GuardedRedisConnection
