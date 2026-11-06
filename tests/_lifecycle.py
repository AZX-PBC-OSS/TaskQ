"""The Task/Future ownership slice of the lifecycle design.

A scope tree (invocation, module, fixture, test, attempt) holds ordered
resource leases. Every task or Future a test's code creates on a
pytest-controlled loop is registered in the scope that is active in its
creating context (the task factory stamps the registry; the active scope
travels in the task context, so descendants, done callbacks, and plain
Futures land in the same scope). The teardown protocol per scope: freeze
admission, stop producers through their domain protocol, a bounded
natural grace, await and retrieve all registered work, abort by
cancellation, a second bounded wait, then report survivors and poison
the scope. Isolation and successful cleanup are separate: a non-release
lease that outlived its test's assertions still fails the test even when
the drain completed it.

This slice covers tasks, Futures/callbacks, managed loops, and the
asyncpg release tail only. Threads, subprocesses, private loops, raw
connections, containers, and the product's ``shield_with_retrieval``
tails are separate changes. The static guard is a tripwire over the
converted drivers; this registry is the guarantee.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import types
from collections.abc import AsyncIterator, Awaitable, Callable
from contextvars import Context, ContextVar
from dataclasses import dataclass, field
from enum import StrEnum

GRACE_SECONDS = 30.0
NATURAL_GRACE_SECONDS = 5.0
DRAIN_BUDGET_SECONDS = 10.0
FALLBACK_WAIT_SECONDS = 2.0


class ScopeKind(StrEnum):
    INVOCATION = "invocation"
    MODULE = "module"
    FIXTURE = "fixture"
    TEST = "test"
    ATTEMPT = "attempt"


class Phase(StrEnum):
    SETUP = "setup"
    CALL = "call"
    TEARDOWN = "teardown"


class Disposition(StrEnum):
    """The outcome of one lease's lifecycle, as the policy reads it."""

    CLEAN = "clean"
    RELEASE_TAIL = "release-tail"
    FAILED = "failed"
    ABORTED = "aborted"
    SURVIVOR = "survivor"


_active_scope: ContextVar[LifecycleScope | None] = ContextVar(
    "lifecycle_active_scope", default=None
)
#: The conftest drain's alias: the ownership stamp the drain reads.
_call_owner = _active_scope


#: The scope for tasks created on a controlled loop outside any active
#: scope (the pytest-asyncio machinery's own drivers). The guard fails
#: any pending quarantine lease closed.
def current_scope() -> LifecycleScope | None:
    """The scope active in the current context."""
    return _active_scope.get()


def _is_asyncpg_release(task: asyncio.Task[object]) -> bool:
    """The installed asyncpg pool release, by code-object identity."""
    code = getattr(task.get_coro(), "cr_code", None)
    release_code = _asyncpg_release_code_object()
    return code is not None and code is release_code


_asyncpg_release_code: types.CodeType | None = None
_asyncpg_release_code_loaded = False


def _asyncpg_release_code_object() -> types.CodeType | None:
    """The installed release code object, loaded once; ``None`` when
    asyncpg cannot be imported (the allowlist is then empty)."""
    global _asyncpg_release_code, _asyncpg_release_code_loaded
    if not _asyncpg_release_code_loaded:
        _asyncpg_release_code_loaded = True
        try:
            import asyncpg.pool

            _asyncpg_release_code = asyncpg.pool.PoolConnectionHolder.release.__code__
        except Exception:  # Why: any import failure empties the allowlist; the drain then fails every drained leak loudly instead of allowing it.
            _asyncpg_release_code = None
    return _asyncpg_release_code


def classify(lease: ResourceLease) -> Disposition:
    """The disposition of one completed lease: the release tails are
    benign; a task that errored failed; the rest cleaned."""
    if lease.task is not None:
        if lease.task.cancelled():
            return Disposition.ABORTED
        if _is_asyncpg_release(lease.task):
            return Disposition.RELEASE_TAIL
        exc = lease.task.exception()
        if exc is not None:
            return Disposition.FAILED
    return Disposition.CLEAN


@dataclass
class ManagedLoop:
    """The supervised loop's task and its shutdown event."""

    task: asyncio.Task[object]
    shutdown: asyncio.Event


@dataclass(eq=False)
class ResourceLease:
    """One piece of test-owned work: a task, a managed loop, or a
    Future, with its stop/abort operations and its final disposition."""

    scope: LifecycleScope
    kind: str
    phase: Phase
    label: str
    task: asyncio.Task[object] | None = None
    future: asyncio.Future[object] | None = None
    shutdown: asyncio.Event | None = None
    abrupt_subject: bool = False
    disposition: Disposition | None = None
    retrieved: bool = False
    created: float = field(default_factory=time.monotonic)

    @property
    def name(self) -> str:
        """The lease's live name. A task's name is set by the loop's
        ``create_task`` after the factory returns, so the registry reads
        it through; an unnamed task ends up literally ``'None'`` there
        (the factory branch calls ``set_name(None)``), and falls back to
        the placeholder captured at registration."""
        if self.task is None:
            return self.label
        live = self.task.get_name()
        return self.label if live == "None" else live

    def stop(self, grace: float = GRACE_SECONDS) -> None:
        """The domain protocol's graceful stop: the shutdown event."""
        if self.shutdown is not None:
            self.shutdown.set()

    def abort(self) -> None:
        """Abort: cancellation for tasks, no-op for bare futures."""
        if self.task is not None and not self.task.done():
            self.task.cancel()

    async def await_outcome(self, bound: float) -> None:
        """Wait up to *bound* for the work to finish and retrieve its
        outcome (so asyncio never reports it unretrieved)."""
        target: asyncio.Future[object] | None = (
            self.task if self.task is not None else self.future
        )
        if target is None:
            return
        await asyncio.wait({target}, timeout=bound)
        if target.done():
            with contextlib.suppress(asyncio.CancelledError):
                target.exception()
            self.retrieved = True

    def describe(self) -> str:
        state = "done" if (self.task is None or self.task.done()) else "pending"
        return f"  - {self.kind} {self.name!r} ({self.phase.value}, {state})"


@dataclass(eq=False)
class LifecycleScope:
    """A node in the ownership tree: the leases created while the scope
    is active in the creating context belong to it."""

    kind: ScopeKind
    name: str
    parent: LifecycleScope | None = None
    leases: list[ResourceLease] = field(default_factory=list)
    children: list[LifecycleScope] = field(default_factory=list)
    poisoned: bool = False
    closed: bool = False

    def child(self, kind: ScopeKind, name: str) -> LifecycleScope:
        child = LifecycleScope(kind=kind, name=name, parent=self)
        self.children.append(child)
        return child

    def lease(
        self,
        kind: str,
        phase: Phase,
        name: str,
        *,
        task: asyncio.Task[object] | None = None,
        future: asyncio.Future[object] | None = None,
        shutdown: asyncio.Event | None = None,
        abrupt_subject: bool = False,
    ) -> ResourceLease:
        lease = ResourceLease(
            scope=self,
            kind=kind,
            phase=phase,
            label=name,
            task=task,
            future=future,
            shutdown=shutdown,
            abrupt_subject=abrupt_subject,
        )
        self.leases.append(lease)
        return lease

    def pending(self) -> list[ResourceLease]:
        # The task running a close (a fixture finalizer driver the
        # factory registered, because its captured context carries the
        # fixture scope) is the close's own machinery, never its
        # subject: waiting on it would wait on ourselves.
        current = asyncio.current_task()
        return [
            lease
            for lease in self.leases
            if (lease.task is None or lease.task is not current)
            and not (lease.task is not None and lease.task.done())
            and not (lease.future is not None and lease.future.done())
        ]

    def tree_leases(self) -> list[ResourceLease]:
        leases = list(self.leases)
        for child in self.children:
            leases.extend(child.tree_leases())
        return leases

    async def close(
        self,
        *,
        grace: float = GRACE_SECONDS,
        drain: Callable[[set[asyncio.Future[object]]], Awaitable[object]] | None = None,
    ) -> list[ResourceLease]:
        """The teardown protocol, in order: freeze (the caller's code has
        returned, so no new work is admitted), stop the producers through
        their domain protocol, the bounded natural grace, the caller's
        drain over the still-pending registered work, abort, a second
        bounded wait, then the survivors poison the scope. A lease the
        scope itself aborted ends ABORTED; a lease that was already done
        at close entry ends CLEAN, RELEASE_TAIL, or FAILED. Returns the
        survivor list."""
        self.closed = True
        # The task running this close (a fixture finalizer driver the
        # factory registered, because its captured context carries the
        # fixture scope) is the close's own machinery, never its subject.
        current = asyncio.current_task()

        def wait_set(leases: list[ResourceLease]) -> set[asyncio.Future[object]]:
            out: set[asyncio.Future[object]] = set()
            for lease in leases:
                if lease.task is not None:
                    out.add(lease.task)
                elif lease.future is not None:
                    out.add(lease.future)
            return out

        pending = self.pending()
        for lease in pending:
            lease.stop()
        if pending:
            await asyncio.wait(wait_set(pending), timeout=min(NATURAL_GRACE_SECONDS, grace))
        pending = self.pending()
        if drain is not None and pending:
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(drain(wait_set(pending)), timeout=grace)
        aborting = self.pending()
        for lease in aborting:
            lease.abort()
        if aborting:
            await asyncio.wait(wait_set(aborting), timeout=min(FALLBACK_WAIT_SECONDS, grace))
        survivors = self.pending()
        for lease in survivors:
            lease.disposition = Disposition.SURVIVOR
            self.poisoned = True
        for lease in aborting:
            if lease.disposition is None:
                lease.disposition = Disposition.ABORTED
        for lease in self.leases:
            if lease.task is not None and lease.task is current:
                if lease.disposition is None:
                    lease.disposition = Disposition.CLEAN
                lease.retrieved = True
                continue
            if lease.disposition is not None:
                continue
            exception: BaseException | None = None
            unretrieved = False
            if lease.task is not None:
                if not lease.task.cancelled():
                    exception = lease.task.exception()
                    # An exception another consumer already retrieved (an
                    # await, the domain's own reap) was seen; only one no
                    # one ever consumed is this guard's finding. The flag
                    # is asyncio's own unretrieved marker.
                    unretrieved = bool(getattr(lease.task, "_log_traceback", False))
                lease.retrieved = True
            elif lease.future is not None and lease.future.done():
                exception = lease.future.exception()
                unretrieved = bool(getattr(lease.future, "_log_traceback", False))
                lease.retrieved = True
            if lease.task is not None and _is_asyncpg_release(lease.task):
                lease.disposition = Disposition.RELEASE_TAIL
            elif exception is not None and unretrieved:
                lease.disposition = Disposition.FAILED
            else:
                lease.disposition = Disposition.CLEAN
        return survivors


#: The scope for tasks created on a controlled loop outside any active
#: scope (the pytest-asyncio machinery's own drivers). The guard fails
#: any pending quarantine lease closed.
QUARANTINE = LifecycleScope(kind=ScopeKind.INVOCATION, name="quarantine")


def current_scope_lease_for(task: asyncio.Task[object]) -> ResourceLease | None:
    return getattr(task, "_tq_lease", None)


def install_lease_factory(loop: asyncio.AbstractEventLoop) -> None:
    """Install, once per loop, the task factory that registers every
    created task/Future in the active scope of its creating context.
    Tasks created outside any scope land in the registry's quarantine
    and fail the guard closed."""
    if getattr(loop, "_tq_lease_factory_installed", False):
        return

    def _lease_factory(
        loop: asyncio.AbstractEventLoop,
        coro: asyncio.Future[object] | Awaitable[object],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[object]:
        from contextvars import copy_context

        task_context = context if context is not None else copy_context()
        scope = task_context.get(_active_scope)
        task_kwargs: dict[str, object] = {"loop": loop, "context": task_context}
        if name is not None:
            task_kwargs["name"] = name
        task = asyncio.Task(coro, **task_kwargs)  # type: ignore[arg-type]
        phase = Phase.CALL if scope is not None and scope.kind is ScopeKind.TEST else Phase.SETUP
        lease = (scope or QUARANTINE).lease("task", phase, task.get_name(), task=task)
        task._tq_lease = lease  # type: ignore[attr-defined]  # Why: the lease is the task's lifecycle record; Task has no extension point and the registry enumerates leases, not live task sets.
        return task

    loop.set_task_factory(_lease_factory)
    loop._tq_lease_factory_installed = True  # type: ignore[attr-defined]  # Why: the idempotence marker lives on the loop; the module loops are plain asyncio loops.


@contextlib.asynccontextmanager  # pyright: ignore[reportDeprecated]  # Why: the stdlib cm factory is the test-utility shape the suite's other async CMs use.
async def managed_loop(
    loop_fn: Callable[[asyncio.Event], Awaitable[object]],
    *,
    grace: float = GRACE_SECONDS,
    drain: Callable[[], Awaitable[object]] | None = None,
    name: str | None = None,
) -> AsyncIterator[ManagedLoop]:
    """The managed-loop adapter over the registry: a supervised loop lease
    with the graceful stop, the two-phase bound, the caller drain, and the
    body > loop > drain precedence."""
    scope = current_scope()
    if scope is None:
        scope = QUARANTINE
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop_fn(shutdown), name=name)
    # The factory already leased this task in the active scope; the
    # supervisor refines that lease instead of registering a second one.
    lease = getattr(task, "_tq_lease", None)
    if lease is None:
        lease = scope.lease(
            "managed-loop",
            Phase.CALL,
            name or task.get_name(),
            task=task,
            shutdown=shutdown,
        )
    else:
        lease.kind = "managed-loop"
        lease.shutdown = shutdown
    body_exc: BaseException | None = None
    try:
        yield ManagedLoop(task=task, shutdown=shutdown)
    except BaseException as exc:
        body_exc = exc
        raise
    finally:
        lease.stop(grace)
        survivor = False
        loop_exc: BaseException | None = None
        done, _pending = await asyncio.wait({task}, timeout=grace)
        if done:
            if not task.cancelled():
                try:
                    task.result()
                except BaseException as exc:
                    loop_exc = exc
        else:
            lease.abort()
            _, still = await asyncio.wait({task}, timeout=2.0)
            survivor = bool(still)
        if drain is not None:
            if body_exc is None and loop_exc is None:
                await asyncio.wait_for(drain(), timeout=grace)
            else:
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(drain(), timeout=grace)
        lease.disposition = (
            Disposition.SURVIVOR
            if survivor
            else Disposition.FAILED
            if loop_exc is not None
            else Disposition.CLEAN
        )
        if loop_exc is not None:
            with contextlib.suppress(asyncio.CancelledError):
                task.exception()
        if body_exc is None and loop_exc is not None:
            raise loop_exc
        if survivor and body_exc is None:
            raise RuntimeError(
                f"the supervised loop {task.get_name()!r} did not finish "
                "within its grace and ignored its cancellation"
            )
