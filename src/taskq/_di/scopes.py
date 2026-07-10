"""Concrete ScopeContainer with LIFO teardown and factory-shape dispatch.

Implements the log-and-continue teardown policy: each teardown callback
runs in its own try/except; failures are logged at ERROR; remaining
teardowns always fire. A parallel ``_teardowns`` list replaces
``AsyncExitStack.aclose()`` which re-raises the first exception and
swallows the rest (research line 743-758).
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, assert_never, cast

import structlog
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.solver import solve_dependencies
from taskq._di.types import FactoryShape, ProviderEntry, ProviderLifecycle
from taskq._di.types import ScopeContainer as ScopeContainerProtocol
from taskq.context import JobContext
from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq._di.scopes")

# Why: resolver accepts object (erasure boundary — entry.impl is object per
# ) and returns Any (the kwargs dict shape depends
# on the factory's signature, which the resolver inspects at runtime).
_Resolver = Callable[[object], Any]


def make_resolver(
    registry: ProviderRegistry,
    scope_containers: dict[Scope, ScopeContainerProtocol],
) -> _Resolver:
    """Construct the resolver callable expected by ScopeContainer.__init__.

    The resolver invokes solve_dependencies with the registry and scope
    containers. The closure captures scope_containers by reference so the
    resolver sees containers added after its construction (the dict is
    populated incrementally as each scope container is bootstrapped).
    """

    def _resolver(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    return _resolver


class ScopeContainer:
    """Concrete scope-lifetime container owning a cache, teardown list, and AsyncExitStack.

    The container is responsible for ALL factory invocation, caching, and
    teardown registration. The solver engine NEVER calls a factory directly
    and NEVER touches an AsyncExitStack.
    """

    def __init__(
        self,
        *,
        scope: Scope,
        resolver: _Resolver,
    ) -> None:
        self._scope: Scope = scope
        self._cache: dict[type, object] = {}
        self._stack: AsyncExitStack = AsyncExitStack()
        self._teardowns: list[Callable[[], Any]] = []
        self._resolver: _Resolver = resolver
        self._sync_gen_executor: ThreadPoolExecutor | None = None
        self._last_cache_hit: bool = False

    @property
    def last_cache_hit(self) -> bool:
        """Whether the most recent ``get_or_create`` returned a cached value."""
        return self._last_cache_hit

    async def get_or_create[T](self, type_: type[T], entry: ProviderEntry[T]) -> T:
        """Resolve *type_* via *entry*, creating and caching the instance if needed."""
        if self._scope is not Scope.TRANSIENT:
            cached = self._cache.get(type_)
            if cached is not None:
                self._last_cache_hit = True
                return cached  # type: ignore[return-value]  # Why: _cache is dict[type, object]; caller's T is recovered via the ProviderEntry[T] that selected this branch
        self._last_cache_hit = False

        match entry.factory_shape:
            case FactoryShape.VALUE:
                result: object = entry.impl
            case FactoryShape.SYNC_CALLABLE:
                kwargs = await self._resolver(entry.impl)
                result = cast(Callable[..., Any], entry.impl)(**kwargs)
            case FactoryShape.ASYNC_CALLABLE:
                kwargs = await self._resolver(entry.impl)
                result = await cast(Callable[..., Any], entry.impl)(**kwargs)
            case FactoryShape.SYNC_GENERATOR:
                result = await self._resolve_sync_generator(entry)
            case FactoryShape.ASYNC_GENERATOR:
                result = await self._resolve_async_generator(entry)
            case FactoryShape.CLASS:
                kwargs = await self._resolver(cast(type[Any], entry.impl).__init__)
                instance = cast(type[Any], entry.impl)(**kwargs)
                match entry.lifecycle:
                    case ProviderLifecycle.AsyncContextManager:
                        value = await instance.__aenter__()

                        async def _acm_teardown() -> None:
                            await instance.__aexit__(None, None, None)

                        self._teardowns.append(_acm_teardown)
                        result = value
                    case ProviderLifecycle.AsyncCloseable:
                        self._teardowns.append(instance.aclose)
                        result = instance
                    case ProviderLifecycle.SyncCloseable:
                        self._teardowns.append(lambda inst=instance: asyncio.to_thread(inst.close))
                        result = instance
                    case ProviderLifecycle.Plain | None:
                        result = instance
                    case (
                        ProviderLifecycle.AsyncGenerator
                        | ProviderLifecycle.SyncGenerator
                        | ProviderLifecycle.PlainFactory
                    ):
                        msg = f"factory lifecycle {entry.lifecycle!r} reached CLASS arm"
                        raise RuntimeError(msg)
                    case _:
                        assert_never(entry.lifecycle)
            case _:
                assert_never(entry.factory_shape)

        if self._scope is not Scope.TRANSIENT:
            self._cache[type_] = result

        return result  # type: ignore[return-value]  # Why: same recovery as cache-hit branch — erasure boundary documented

    async def _resolve_sync_generator(self, entry: ProviderEntry[object]) -> object:
        """Resolve a SYNC_GENERATOR provider via a pinned single-thread executor."""
        if self._sync_gen_executor is None:
            self._sync_gen_executor = ThreadPoolExecutor(max_workers=1)
            logger.info("sync-generator-executor-created", scope=self._scope.name)

        kwargs = await self._resolver(entry.impl)
        factory = cast(Callable[..., Generator[Any, None, None]], entry.impl)
        cm = contextlib.contextmanager(factory)(**kwargs)

        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(self._sync_gen_executor, cm.__enter__)

        def _teardown() -> Any:
            return loop.run_in_executor(self._sync_gen_executor, cm.__exit__, None, None, None)

        self._teardowns.append(_teardown)
        return value

    async def _resolve_async_generator(self, entry: ProviderEntry[object]) -> object:
        """Resolve an ASYNC_GENERATOR provider via asynccontextmanager, entering manually."""
        kwargs = await self._resolver(entry.impl)
        factory = cast(Callable[..., AsyncGenerator[Any]], entry.impl)
        cm = asynccontextmanager(factory)(**kwargs)
        value = await cm.__aenter__()

        async def _teardown() -> None:
            await cm.__aexit__(None, None, None)

        self._teardowns.append(_teardown)
        return value

    async def aclose(self) -> None:
        """Close the container with the log-and-continue teardown policy."""
        pending_cancel: BaseException | None = None
        while self._teardowns:
            cb = self._teardowns.pop()
            try:
                await cb()
            except BaseException as exc:
                logger.error(
                    "provider-teardown-error",
                    scope=self._scope.name,
                    exc_info=True,
                )
                # Why: remember a CancelledError so we can re-raise it after
                # all remaining teardowns have run; non-cancel exceptions
                # are logged and dropped  log-and-continue.
                if isinstance(exc, asyncio.CancelledError):
                    pending_cancel = exc

        # Why: shut down the pinned SYNC_GENERATOR executor AFTER all
        # per-provider teardown callbacks have run. shutdown(wait=True) is
        # blocking; running it inline would block the loop .
        if self._sync_gen_executor is not None:
            executor = self._sync_gen_executor
            self._sync_gen_executor = None
            try:
                await asyncio.to_thread(executor.shutdown, True)
            except BaseException as exc:
                # Why: parallels the per-callback BaseException pattern above.
                logger.error(
                    "sync-generator-executor-shutdown-error",
                    scope=self._scope.name,
                    exc_info=True,
                )
                if isinstance(exc, asyncio.CancelledError):
                    pending_cancel = exc
            else:
                logger.info(
                    "sync_generator_executor_shutdown",
                    scope=self._scope.name,
                )

        # Why: after every teardown attempted, re-raise any deferred
        # CancelledError so the outer task's cancellation contract is honored.
        if pending_cancel is not None:
            raise pending_cancel


class ProcessScope(ScopeContainer):
    """PROCESS-lifetime scope — worker process startup → exit."""

    def __init__(self, *, resolver: _Resolver) -> None:
        super().__init__(scope=Scope.PROCESS, resolver=resolver)

    async def bootstrap(
        self,
        registry: ProviderRegistry,
        settings: WorkerSettings,  # Why: accepted for API symmetry with the public surface; bootstrap doesn't use it directly — registration is a separate concern owned by worker bootstrap
    ) -> None:
        """Resolve all PROCESS-scoped providers.

        Why: no try/except around get_or_create — earlier providers'
        teardowns are already registered on self._teardowns; the
        caller's AsyncExitStack runs aclose() on unwind, which
        iterates whatever teardowns were registered before the
        failure. Partial-bootstrap leaks are not possible: every
        successful get_or_create has its teardown queued before
        the next get_or_create starts.
        """
        process_providers = [t for t, e in registry.providers.items() if e.scope is Scope.PROCESS]
        for t in process_providers:
            await self.get_or_create(t, registry.get(t))  # type: ignore[reportUnknownArgumentType]  # Why: registry.get() returns ProviderEntry[Unknown] when called with plain type; runtime guarantee from providers dict iteration
        logger.info(
            "process-scope-opened",
            provider_count=len(process_providers),
        )

    async def shutdown(self) -> None:
        await self.aclose()
        logger.info("process-scope-closed")

    def get(self, type_: type) -> object | None:
        return self._cache.get(type_)


class ThreadScope(ScopeContainer):
    """THREAD-lifetime scope — placeholder for multi-thread workers (trivially empty in M3)."""

    def __init__(self, *, resolver: _Resolver) -> None:
        super().__init__(scope=Scope.THREAD, resolver=resolver)

    async def bootstrap(
        self,
        registry: ProviderRegistry,
        process_scope: ProcessScope,
    ) -> None:
        """Resolve all THREAD-scoped providers (empty in M3 single-thread deployment)."""
        thread_providers = [t for t, e in registry.providers.items() if e.scope is Scope.THREAD]
        for t in thread_providers:
            await self.get_or_create(t, registry.get(t))  # type: ignore[reportUnknownArgumentType]  # Why: registry.get() returns ProviderEntry[Unknown] when called with plain type; runtime guarantee from providers dict iteration
        logger.info(
            "thread-scope-opened",
            provider_count=len(thread_providers),
        )

    async def shutdown(self) -> None:
        await self.aclose()
        logger.info("thread-scope-closed")

    def get(self, type_: type) -> object | None:
        return self._cache.get(type_)


class LoopScope(ScopeContainer):
    """LOOP-lifetime scope — worker loop start → loop close."""

    def __init__(self, *, resolver: _Resolver) -> None:
        super().__init__(scope=Scope.LOOP, resolver=resolver)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    async def bootstrap(
        self,
        registry: ProviderRegistry,
        process_scope: ProcessScope,
        thread_scope: ThreadScope | None = None,
    ) -> None:
        """Resolve all LOOP-scoped providers.

        Why: process_scope / thread_scope parameters are accepted for
        API completeness even though bootstrap doesn't read them
        directly — wider scopes' instances are reachable through the
        _resolver callable (which holds the registry + container map).
        """
        loop_providers = [t for t, e in registry.providers.items() if e.scope is Scope.LOOP]
        for t in loop_providers:
            await self.get_or_create(t, registry.get(t))  # type: ignore[reportUnknownArgumentType]  # Why: registry.get() returns ProviderEntry[Unknown] when called with plain type; runtime guarantee from providers dict iteration
        logger.info(
            "loop-scope-opened",
            provider_count=len(loop_providers),
        )

    async def shutdown(self) -> None:
        await self.aclose()
        logger.info("loop-scope-closed")

    def resolved_cache(self) -> Mapping[type, object]:
        """Return a read-only view of the LOOP-scope resolved values.

        The returned mapping is a live view onto the underlying cache —
        providers resolved after this method is called (e.g., lazy
        providers) become visible without re-fetching. Callers MUST NOT
        attempt to mutate the mapping; doing so raises ``TypeError``.

        Stability invariant: once ``bootstrap`` returns, the registered
        set of LOOP-scope provider types is FROZEN for the lifetime of
        the loop. The values stored under each type key (e.g. an
        ``asyncpg.Connection``) are expected to be stable references —
        callers that read the mapping repeatedly (such as
        ``SubJobEnqueuer.enqueue``) assume the connection identity does
        not change between dispatches. If a future feature needs to
        swap a LOOP-scope value mid-loop (e.g., reconnect after a pool
        reset), it must coordinate with consumers of this mapping
        because the consumer's per-dispatch transaction lifecycle
        requires the same connection across both transaction-open and
        transaction-close.
        """
        return MappingProxyType(self._cache)

    async def get_or_create[T](self, type_: type[T], entry: ProviderEntry[T]) -> T:
        running = asyncio.get_running_loop()
        if running is not self._loop:
            raise RuntimeError(f"LoopScope created on {self._loop!r} but accessed from {running!r}")
        return await super().get_or_create(type_, entry)

    def get(self, type_: type) -> object | None:
        return self._cache.get(type_)


@dataclass(frozen=True, slots=True)
class ResolvedActorScope:
    """The yielded value of build_actor_scope.

    ctx        — the JobContext supplied by the consumer (passthrough).
    di_kwargs  — the DI-resolved kwargs dict for the actor function.

    Usage::

        async with build_actor_scope(...) as resolved:
            await run_actor(job, resolved.ctx, **resolved.di_kwargs)
    """

    ctx: JobContext[BaseModel]
    di_kwargs: dict[str, object]


@asynccontextmanager
async def build_actor_scope(
    *,
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
    actor_func: Callable[..., Awaitable[object]],
    actor_name: str,
    passthrough_kwargs: dict[str, object],
) -> AsyncGenerator[ResolvedActorScope, None]:
    """Per-invocation actor scope: opens TRANSIENT stack, resolves DI kwargs.

    Yields ResolvedActorScope(ctx, di_kwargs). On exit (regardless of
    outcome), closes the TRANSIENT stack in LIFO order via the
    log-and-continue teardown policy.

    passthrough_kwargs MUST contain the JobContext (key "ctx") and the
    validated payload (key "payload"). The consumer constructs both
    per-job and supplies them as passthrough; the registry's graph walk
    is configured to skip these parameter names, so they are never
    resolved from providers.
    """
    scope_containers: dict[Scope, ScopeContainerProtocol] = {}

    def _resolver(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    transient_scope = ScopeContainer(scope=Scope.TRANSIENT, resolver=_resolver)

    scope_containers = {
        Scope.PROCESS: process_scope,
        Scope.THREAD: thread_scope,
        Scope.LOOP: loop_scope,
        Scope.TRANSIENT: transient_scope,
    }

    # Why: the resolver closure captured scope_containers before TRANSIENT
    # was added; re-bind so the resolver sees all four containers.
    def _resolver_with_all(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    transient_scope._resolver = _resolver_with_all  # pyright: ignore[reportPrivateUsage]  # Why: build_actor_scope constructs the TRANSIENT container and must wire its resolver to see all four scope containers; the resolver is a closure detail owned by this call site

    logger.info("transient-scope-opened", actor_name=actor_name)
    try:
        di_kwargs = await solve_dependencies(
            func=actor_func,
            registry=registry,
            scope_containers=scope_containers,
            passthrough_kwargs=passthrough_kwargs,
        )
        # Why: solve_dependencies includes passthrough keys in its result
        # dict; build_actor_scope yields ctx separately and the consumer
        # spreads di_kwargs — so passthrough keys must not appear in
        # di_kwargs to avoid duplicate keyword arguments at the call site.
        for _key in passthrough_kwargs:
            di_kwargs.pop(_key, None)
        ctx = passthrough_kwargs["ctx"]
        if not isinstance(ctx, JobContext):
            raise TypeError(
                f"passthrough_kwargs['ctx'] must be a JobContext, got {type(ctx).__name__}"
            )
        yield ResolvedActorScope(ctx=ctx, di_kwargs=di_kwargs)  # type: ignore[reportUnknownArgumentType]  # Why: ctx is narrowed to JobContext[Any] by isinstance but pyright cannot recover the BaseModel bound from the passthrough_kwargs dict[str, object] — the consumer that built passthrough_kwargs guarantees the correct P at the call site
    finally:
        # Why: shield the TRANSIENT teardown so cancellation /
        # asyncio.wait_for timeouts in the with-body do not
        # short-circuit the teardown mid-way.
        # ("wrap terminal writes in asyncio.shield") applies to
        # scope teardown too — losing teardown of an opened
        # resource leaks it. CancelledError after the shielded
        # aclose finishes is re-raised to honor the outer cancel.
        try:
            await asyncio.shield(transient_scope.aclose())
        except asyncio.CancelledError:
            raise
        finally:
            logger.info("transient-scope-closed", actor_name=actor_name)
