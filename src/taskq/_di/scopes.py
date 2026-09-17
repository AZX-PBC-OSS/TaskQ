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
from contextlib import asynccontextmanager
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
from taskq._shield import shield_with_retrieval
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
    """Concrete scope-lifetime container owning a cache and a teardown list.

    The container is responsible for ALL factory invocation, caching, and
    teardown registration. The solver engine NEVER calls a factory directly
    and NEVER touches an AsyncExitStack.
    """

    def __init__(
        self,
        *,
        scope: Scope,
        resolver: _Resolver,
        factory_timeout: float | None = None,
    ) -> None:
        self._scope: Scope = scope
        self._cache: dict[type, object] = {}
        self._teardowns: list[Callable[[], Any]] = []
        self._resolver: _Resolver = resolver
        self._sync_gen_executor: ThreadPoolExecutor | None = None
        self._last_cache_hit: bool = False
        self._factory_timeout: float | None = factory_timeout

    async def _await_factory[T](self, coro: Awaitable[T], *, type_: type) -> T:
        """Await a user-registered factory's first-use open, bounded.

        ``factory_timeout`` is the bounded-open discipline applied at the DI
        registry's own factory seam: a user factory that accepts the
        call and never returns (the black-holed credential endpoint —
        the documented "database pools, HTTP clients" shape) must fail
        the operation that awaited it within the configured bound, not
        park it forever. The worker's bootstrap passes
        ``reload_factory_timeout`` here for every scope it opens — the
        pre-watchdog window where no other bound exists — and the
        timeout's ``TimeoutError`` propagates to fail that boot loudly;
        ``None`` (the default) keeps the unbounded await for containers
        whose callers supply their own bounds (the per-job TRANSIENT
        scope, whose factories run under the consumer's deadline and
        the armed watchdogs).
        """
        if self._factory_timeout is None:
            return await coro
        bound = asyncio.timeout(self._factory_timeout)
        try:
            async with bound:
                return await coro
        except TimeoutError:
            if not bound.expired():
                # The factory's own TimeoutError, not this bound —
                # re-raise unlabeled so the caller sees its real cause.
                raise
            logger.error(
                "di-factory-first-use-timeout",
                kind="di_factory_first_use_timeout",
                provider_type=type_.__qualname__,
                scope=self._scope.name,
                timeout=self._factory_timeout,
            )
            raise

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
                factory_coro = cast(Callable[..., Any], entry.impl)(**kwargs)
                result = await self._await_factory(factory_coro, type_=type_)
            case FactoryShape.SYNC_GENERATOR:
                result = await self._resolve_sync_generator(entry)
            case FactoryShape.ASYNC_GENERATOR:
                result = await self._resolve_async_generator(entry)
            case FactoryShape.CLASS:
                kwargs = await self._resolver(cast(type[Any], entry.impl).__init__)
                instance = cast(type[Any], entry.impl)(**kwargs)
                match entry.lifecycle:
                    case ProviderLifecycle.AsyncContextManager:
                        enter_coro = instance.__aenter__()
                        value = await self._await_factory(enter_coro, type_=type_)

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
        # Capture the executor for the teardown closure: aclose() nulls the
        # attribute after the teardown pass, and the callback must keep
        # working on the executor this provider's enter ran on regardless.
        executor = self._sync_gen_executor

        kwargs = await self._resolver(entry.impl)
        factory = cast(Callable[..., Generator[Any, None, None]], entry.impl)
        cm = contextlib.contextmanager(factory)(**kwargs)

        loop = asyncio.get_running_loop()
        enter_future = loop.run_in_executor(executor, cm.__enter__)
        value = await self._await_factory(enter_future, type_=entry.type_)

        async def _teardown() -> None:
            exit_future = loop.run_in_executor(executor, cm.__exit__, None, None, None)
            if self._factory_timeout is None:
                # Same policy as _await_factory: the caller (the per-job
                # TRANSIENT scope's consumer) owns the bound.
                await exit_future
                return
            bound = asyncio.timeout(self._factory_timeout)
            try:
                async with bound:
                    await exit_future
            except TimeoutError:
                if not bound.expired():
                    # The generator's own __exit__ raised TimeoutError —
                    # surface it as the teardown failure it is, not as the
                    # bound firing.
                    raise
                # The __exit__ is parked in unkillable user code on the
                # executor's thread. Log-and-continue like every teardown
                # failure: the residue is that one thread until the code
                # returns, and what scope teardown must never do is park
                # every teardown after it on the wait.
                logger.error(
                    "sync-generator-teardown-timeout",
                    kind="sync_generator_teardown_timeout",
                    scope=self._scope.name,
                    provider_type=entry.type_.__qualname__,
                    timeout=self._factory_timeout,
                )

        self._teardowns.append(_teardown)
        return value

    async def _resolve_async_generator(self, entry: ProviderEntry[object]) -> object:
        """Resolve an ASYNC_GENERATOR provider via asynccontextmanager, entering manually."""
        kwargs = await self._resolver(entry.impl)
        factory = cast(Callable[..., AsyncGenerator[Any]], entry.impl)
        cm = asynccontextmanager(factory)(**kwargs)
        enter_coro = cm.__aenter__()
        value = await self._await_factory(enter_coro, type_=entry.type_)

        async def _teardown() -> None:
            await cm.__aexit__(None, None, None)

        self._teardowns.append(_teardown)
        return value

    @property
    def has_teardown_work(self) -> bool:
        """Whether :meth:`aclose` has anything left to run.

        False once every registered teardown has run and the pinned
        SYNC_GENERATOR executor (if one was ever opened) is shut down —
        i.e. exactly when ``aclose()`` would return without awaiting
        anything, which lets a per-job caller skip scheduling it.
        """
        return bool(self._teardowns) or self._sync_gen_executor is not None

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
        #
        # The wait itself is bounded by ``factory_timeout`` when the
        # container has one: a sync-gen ``__enter__``/``__exit__`` parked
        # in unkillable user code would otherwise park scope teardown for
        # as long as the user code hangs. The bound trips loudly and lets
        # the close complete — the shutdown already initiated finishes in
        # the background whenever the user code returns, and the residue
        # is that one thread per hung container until then (a leak the
        # stdlib makes unrecoverable by construction; what teardown owns
        # is never parking the REST of the close on it).
        if self._sync_gen_executor is not None:
            executor = self._sync_gen_executor
            self._sync_gen_executor = None
            try:
                if self._factory_timeout is None:
                    await asyncio.to_thread(executor.shutdown, True)
                else:
                    async with asyncio.timeout(self._factory_timeout):
                        await asyncio.to_thread(executor.shutdown, True)
            except TimeoutError:
                logger.error(
                    "sync-generator-executor-shutdown-timeout",
                    kind="sync_generator_executor_shutdown_timeout",
                    scope=self._scope.name,
                    timeout=self._factory_timeout,
                )
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

    def __init__(
        self,
        *,
        resolver: _Resolver,
        factory_timeout: float | None = None,
    ) -> None:
        super().__init__(scope=Scope.PROCESS, resolver=resolver, factory_timeout=factory_timeout)

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

    def __init__(
        self,
        *,
        resolver: _Resolver,
        factory_timeout: float | None = None,
    ) -> None:
        super().__init__(scope=Scope.THREAD, resolver=resolver, factory_timeout=factory_timeout)

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

    def __init__(
        self,
        *,
        resolver: _Resolver,
        factory_timeout: float | None = None,
    ) -> None:
        super().__init__(scope=Scope.LOOP, resolver=resolver, factory_timeout=factory_timeout)
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
        providers) become visible without re-fetching, and replacements
        made via :meth:`replace_value` become visible to subsequent
        ``.get()`` reads. Callers MUST NOT attempt to mutate the mapping;
        doing so raises ``TypeError``.

        Stability invariant: once ``bootstrap`` returns, the registered
        set of LOOP-scope provider types is FROZEN for the lifetime of
        the loop. ``asyncpg.Connection`` values are stable references —
        callers that read the mapping repeatedly (such as
        ``SubJobEnqueuer.enqueue``) assume the connection identity does
        not change between dispatches, because the consumer's
        per-dispatch transaction lifecycle requires the same connection
        across both transaction-open and transaction-close.

        ``asyncpg.Pool`` values are the one sanctioned exception: they
        may be replaced mid-loop via :meth:`replace_value` (credential
        hot-reload — see ``taskq.worker.deps.reload_credentials``).
        Pools are stateless acquisition factories, so swapping the
        reference does not violate any transaction-identity invariant;
        consumers that read the mapping per dispatch (rather than
        capturing it once) always see the current pool.
        """
        return MappingProxyType(self._cache)

    def replace_value(self, type_: type, value: object) -> None:
        """Replace the cached instance for *type_*.

        The sanctioned mid-loop swap for VALUE providers whose resource
        was hot-swapped externally — currently ``asyncpg.Pool`` after a
        SIGHUP credential reload (``taskq.worker.deps.reload_credentials``
        drains and closes the old pool; without this, DI-injected
        ``db: asyncpg.Pool`` consumers would keep using a closed pool).

        The old value is NOT torn down here — its lifecycle belongs to
        the swapper. Only the cache entry is replaced; the (sealed)
        registry is untouched. Raises :class:`KeyError` when *type_* has
        no cached value — replacing an absent entry would silently
        insert a value no provider declared.
        """
        if type_ not in self._cache:
            raise KeyError(f"no cached value for {type_!r} — nothing to replace")
        self._cache[type_] = value

    async def get_or_create[T](self, type_: type[T], entry: ProviderEntry[T]) -> T:
        running = asyncio.get_running_loop()
        if running is not self._loop:
            raise RuntimeError(f"LoopScope created on {self._loop!r} but accessed from {running!r}")
        return await super().get_or_create(type_, entry)

    def get(self, type_: type) -> object | None:
        return self._cache.get(type_)


class LoopScopeSlotView:
    """Per-actor-invocation view of the LOOP scope for one consumer slot.

    The worker's per-slot path acquires a dedicated connection per job (a
    LOOP-scope ``asyncpg.Connection`` registered plus ``max_concurrency >
    1``); the transaction that wraps the actor runs on that connection.
    This view shadows the LOOP-shared cache for exactly the types its
    mapping carries, for exactly one ``build_actor_scope`` body, so the
    actor — and every nested LOOP-scoped resolution its dependencies
    trigger through the same scope-containers map — resolves the slot's
    instance instead of the one object registered at ``Scope.LOOP``.
    Without the shadow, every concurrent slot's actor received the SAME
    registered connection and interleaved operations on it: asyncpg
    permits one operation per connection, so healthy actors raised
    ``InterfaceError`` (misattributed to the actor, retry budget burned),
    and the actor's own writes sat outside its slot's transaction
    (each job's work must run in isolation within its
    transaction; sharing a connection across concurrent jobs violates
    that isolation and causes writes to land outside any job's boundary).

    The shadow reaches one level further than the mapping itself: a
    LOOP-scoped FACTORY whose dependency closure reaches a shadowed type
    (a helper that injects the connection — "any object derived from a
    shared connection is shared the same way", the same rule one level
    removed) is re-resolved PER INVOCATION through the invocation's
    TRANSIENT runner instead of the LOOP cache. The scope's bootstrap
    eagerly resolved that factory through the REAL containers, so the
    cached singleton holds the ONE registered connection; handing it to
    concurrent slots' actors would rebuild the exact sharing the view
    exists to prevent. Re-resolution runs the factory's own parameters
    through the same shadowed scope-containers map (nested LOOP-scoped
    dependencies included), produces an instance this invocation alone
    owns, and lands any lifecycle teardown on the invocation's TRANSIENT
    teardown — the derived object lives and dies with the actor call.
    Providers whose closure never touches a shadowed type keep the LOOP
    singleton: the view does not per-invocation-ize unrelated
    loop-lifetime resources.

    The view is a pure read-through otherwise: it never caches, never
    invokes a factory on its own account, and never registers a
    teardown — a mapped instance's lifecycle belongs to the wiring that
    supplied it (the dispatch acquire/release exit stack that owns the
    slot connection), and a re-resolved instance's to the invocation
    runner. Every type NOT in the mapping and NOT shadow-derived
    resolves through the real LOOP container unchanged, and nothing
    outside this actor invocation can observe the shadow: the LOOP
    cache itself is never touched.

    Mapped values must be live instances; ``None`` is not a meaningful
    mapping (it is treated as absent, exactly like the LOOP cache's own
    miss shape).
    """

    def __init__(
        self,
        inner: LoopScope,
        slot_values: Mapping[type, object],
        *,
        invocation_runner: ScopeContainer,
        shadow_derived: frozenset[type],
    ) -> None:
        self._inner = inner
        self._slot_values = slot_values
        self._invocation_runner = invocation_runner
        self._shadow_derived = shadow_derived
        self._last_cache_hit = False

    async def get_or_create[T](self, type_: type[T], entry: ProviderEntry[T]) -> T:
        value = self._slot_values.get(type_)
        if value is not None:
            self._last_cache_hit = True
            return cast("T", value)  # pyright: ignore[reportReturnType]  # Why: the DI erasure boundary — the mapping's value is the live instance the caller's wiring owns for type_ (dispatch maps the slot connection under asyncpg.Connection); the same value-shape trust ScopeContainer's cache-hit branch applies.
        if type_ in self._shadow_derived:
            # A LOOP-scoped factory derived (transitively) from a
            # shadowed type: the LOOP cache's bootstrap singleton baked
            # in the registered instance, so resolve this invocation's
            # own through the TRANSIENT runner — fresh parameters off
            # the shadowed containers map, no caching, teardown owned
            # by the invocation.
            self._last_cache_hit = False
            return await self._invocation_runner.get_or_create(type_, entry)  # pyright: ignore[reportReturnType]  # Why: same erasure boundary as the mapped branch — the runner's ProviderEntry[T] is the caller's entry; ScopeContainer's return-site coercion covers T.
        self._last_cache_hit = False
        return await self._inner.get_or_create(type_, entry)

    @property
    def last_cache_hit(self) -> bool:
        """Whether the most recent ``get_or_create`` served a mapped or cached value."""
        return self._last_cache_hit

    async def aclose(self) -> None:
        # The view owns no resources; the LOOP container's lifecycle is
        # unchanged by one actor invocation having looked through it.
        await self._inner.aclose()


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
    loop_slot_values: Mapping[type, object] | None = None,
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

    loop_slot_values maps LOOP-registered types to the instances THIS
    consumer slot owns for this one actor invocation (the dispatch path
    maps ``asyncpg.Connection`` to the job's slot connection). Each
    mapped type resolves to the slot's instance — through a
    :class:`LoopScopeSlotView` that shadows the LOOP cache for this
    invocation only, actor parameters and nested LOOP-scoped
    dependencies alike — so a LOOP-registered connection never reaches
    two concurrent slots' actors, and a LOOP-scoped
    factory DERIVED from a shadowed type (a helper holding the
    connection) resolves per invocation instead of serving the
    bootstrap singleton that baked the registered connection in. ``None``
    /empty keeps the plain LOOP container: the single-slot worker, whose
    transaction connection IS the registered connection.
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

    # Why a per-invocation view instead of mutating the LOOP cache: the
    # LOOP cache is shared by every concurrent slot — a swap would race
    # sibling dispatches and leak the slot's connection to later jobs.
    # The view is visible only to the solves that run inside THIS
    # build_actor_scope body, which is exactly the audience the slot's
    # connection is safe for. The invocation runner is the body's own
    # TRANSIENT container: a shadow-derived LOOP factory resolves its
    # parameters through the same containers map (LOOP is this view, so
    # its connection-typed parameters see the slot's instance) and its
    # lifecycle teardowns land on the invocation's teardown — the
    # derived object lives and dies with this one actor call, never
    # cached where a sibling slot could reach it.
    loop_container: LoopScope | LoopScopeSlotView = loop_scope
    if loop_slot_values:
        loop_container = LoopScopeSlotView(
            loop_scope,
            loop_slot_values,
            invocation_runner=transient_scope,
            shadow_derived=registry.shadow_derived_providers(frozenset(loop_slot_values)),
        )

    scope_containers = {
        Scope.PROCESS: process_scope,
        Scope.THREAD: thread_scope,
        Scope.LOOP: loop_container,
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

    # DEBUG, not INFO: this pair fires once per job carrying only the
    # actor name, so at the default level it is a per-job rendering cost
    # for a line nothing consumes.
    logger.debug("transient-scope-opened", actor_name=actor_name)
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
            # shield_with_retrieval, not plain asyncio.shield: a detached
            # teardown that fails under a double cancel must have its
            # outcome retrieved and logged, not lost (see taskq._shield).
            # Skipped outright when the container has nothing to close:
            # most jobs resolve no teardown-bearing provider, and the
            # shield's task creation would be the whole cost.
            if transient_scope.has_teardown_work:
                await shield_with_retrieval(transient_scope.aclose())
        except asyncio.CancelledError:
            raise
        finally:
            logger.debug("transient-scope-closed", actor_name=actor_name)
