"""Loop-pinned workflow dependency products and their actual provider bridge.

The declaration is inert; this module is entered only by an owner on a running
worker loop. A factory receives the real execution context. No product or
provider is stored in a process-global loop proxy.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from taskq._di.scopes import (
    _ProviderSystemExitError,  # pyright: ignore[reportPrivateUsage]  # shared dispatch carrier
)
from taskq._di.types import ProviderEntry, ProviderRegistry
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows.api._deps import deps_satisfied

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "WorkflowDepsContext",
    "WorkflowDepsFactory",
    "WorkflowDepsFactoryResult",
    "WorkflowDepsLifecycleError",
    "WorkflowDepsProductTypeError",
    "WorkflowProviderBridge",
]

logger = get_logger(__name__)

type WorkflowDepsFactoryResult[T] = (
    T | Awaitable[T] | AbstractAsyncContextManager[T] | AsyncGenerator[T, None]
)
type WorkflowDepsFactory[T] = Callable[[WorkflowDepsContext], WorkflowDepsFactoryResult[T]]


class WorkflowDepsLifecycleError(RuntimeError):
    """An owner was asked to serve work after retirement/closure."""


class WorkflowDepsProductTypeError(TypeError):
    """A factory's runtime product contradicts a body's concrete declaration."""


class _ProviderScope(Protocol):
    def get(self, type_: type) -> object | None: ...

    async def get_or_create(self, type_: type[object], entry: ProviderEntry[object]) -> object: ...


class WorkflowProviderBridge:
    """A typed bridge to the worker's real provider scopes.

    Values returned by :meth:`get` remain provider-scope owned. The identity
    ledger prevents a factory returning the parent provider itself from closing
    that provider a second time.
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        *,
        loop_scope: _ProviderScope,
        process_scope: _ProviderScope,
        thread_scope: _ProviderScope | None,
        shared_identities: set[int] | None = None,
    ) -> None:
        self.registry = registry
        self._shared_identities: set[int]
        if shared_identities is None:
            self._shared_identities = set()
        else:
            self._shared_identities = shared_identities
        self._scopes = (
            (loop_scope, "LOOP"),
            (process_scope, "PROCESS"),
            *(() if thread_scope is None else ((thread_scope, "THREAD"),)),
        )

    async def get[T](self, type_: type[T]) -> T:
        """Resolve *type_* through the existing owning scope, never a copy."""
        from taskq._di.scope import Scope

        entry = self.registry.get(cast("type[object]", type_))
        scope_by_name = {name: scope for scope, name in self._scopes}
        selected = scope_by_name.get(entry.scope.name)
        if selected is None:
            raise WorkflowDepsLifecycleError(
                f"provider {type_.__qualname__} has unsupported scope {entry.scope.name}"
            )
        value = selected.get(type_)
        if value is None:
            value = await selected.get_or_create(cast("type[object]", type_), entry)
        if entry.scope is Scope.TRANSIENT:
            raise WorkflowDepsLifecycleError(
                "workflow deps factories cannot borrow TRANSIENT providers: "
                "the worker body transaction/attempt does not share this owner"
            )
        self._note_shared(value)
        return cast("T", value)

    def bind(self, shared_identities: set[int]) -> WorkflowProviderBridge:
        """Attach provider-borrow identity tracking to one product owner."""
        return WorkflowProviderBridge(
            self.registry,
            loop_scope=self._scopes[0][0],
            process_scope=self._scopes[1][0],
            thread_scope=next((scope for scope, name in self._scopes if name == "THREAD"), None),
            shared_identities=shared_identities,
        )

    def _note_shared(self, value: object) -> None:
        self._shared_identities.add(id(value))


@dataclass(frozen=True, slots=True)
class WorkflowDepsContext:
    """Trusted construction inputs for one dependency owner generation.

    ``pool``, ``schema``, ``worker_id``, and ``loop`` are the executing runner's
    actual values. Worker generation follows ``WorkerDeps`` invalidation; a
    direct runner family has its fixed declared generation ``0`` and no provider
    registry. The factory product is created only on first required body use,
    reused within that owner, and closed by the eventual worker/runner close.
    """

    pool: asyncpg.Pool
    schema: str
    worker_id: JobId
    generation: int
    loop: asyncio.AbstractEventLoop
    providers: WorkflowProviderBridge | None = None
    _shared_identities: set[int] = field(default_factory=set[int], repr=False)

    def require_providers(self) -> WorkflowProviderBridge:
        """Fail explicitly on a direct door with no actual worker registry."""
        if self.providers is None:
            raise WorkflowDepsLifecycleError(
                "this deps_factory requires the worker ProviderRegistry bridge; "
                "the direct FlowRunner door has no worker provider scopes"
            )
        return self.providers

    async def provider[T](self, type_: type[T]) -> T:
        return await self.require_providers().get(type_)


@dataclass(slots=True)
class _EnteredProduct:
    product: object
    close: Callable[[], Awaitable[None]] | None
    entry_context: bool = False


async def _await_bounded[T](awaitable: Awaitable[T], bound: float | None) -> T:
    if bound is None:
        return await awaitable
    async with asyncio.timeout(bound):
        return await awaitable


async def _enter_async_generator(
    generator: AsyncGenerator[object, None] | AsyncIterator[object],
    bound: float | None,
) -> _EnteredProduct:
    async def scoped() -> AsyncGenerator[object, None]:
        if isinstance(generator, AsyncGenerator):
            async for value in generator:
                yield value
        else:
            # Runtime backstop for an iterator product: only a closeable async
            # generator has the supported scoped cleanup semantics.
            raise TypeError(
                "deps_factory returned an AsyncIterator, not an async generator; "
                "declare an async-generator factory or an async context manager"
            )

    manager = contextlib.asynccontextmanager(scoped)()
    product = await _await_bounded(manager.__aenter__(), bound)

    async def close() -> None:
        await _await_bounded(manager.__aexit__(None, None, None), bound)

    return _EnteredProduct(product, close, entry_context=True)


async def _enter_value(value: object, bound: float | None) -> _EnteredProduct:
    """Resolve the supported runtime shapes in their actual nesting order."""
    if inspect.isawaitable(value):
        resolved = await _await_bounded(cast("Awaitable[object]", value), bound)
        return await _enter_value(resolved, bound)
    if isinstance(value, (AsyncGenerator, AsyncIterator)):
        generator = cast("AsyncGenerator[object, None] | AsyncIterator[object]", value)
        return await _enter_async_generator(generator, bound)
    if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
        manager = cast("AbstractAsyncContextManager[object]", value)
        product = await _await_bounded(manager.__aenter__(), bound)

        async def close_manager() -> None:
            await _await_bounded(manager.__aexit__(None, None, None), bound)

        return _EnteredProduct(product, close_manager, entry_context=True)
    aclose = getattr(value, "aclose", None)
    if callable(aclose):

        async def close_async() -> None:
            result = aclose()
            if not inspect.isawaitable(result):
                raise TypeError("deps product aclose() must return an awaitable")
            await _await_bounded(cast("Awaitable[object]", result), bound)

        return _EnteredProduct(value, close_async)
    close = getattr(value, "close", None)
    if callable(close):

        async def close_sync() -> None:
            await _await_bounded(asyncio.to_thread(close), bound)

        return _EnteredProduct(value, close_sync)
    return _EnteredProduct(value, None)


class WorkflowDepsOwner:
    """One generation's product, single flight, claim pins and exact-once close."""

    def __init__(
        self,
        factory: object,
        *,
        context: WorkflowDepsContext,
        expected: tuple[tuple[str, object], ...],
        timeout: float | None,
    ) -> None:
        if not callable(factory):
            raise TypeError(f"deps_factory must be callable, got {type(factory).__qualname__}")
        self.factory = factory
        self.context = context
        self.expected = expected
        self.timeout = timeout
        self.loop = context.loop
        self.resolution_count = 0
        self.close_count = 0
        self.retired = False
        self.closed = False
        self._product: object = _UNRESOLVED
        self._close: Callable[[], Awaitable[None]] | None = None
        self._close_is_entry = False
        self._flight: asyncio.Task[None] | None = None
        self._close_flight: asyncio.Task[None] | None = None
        self._pins = 0
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def product(self) -> object:
        if self._product is _UNRESOLVED:
            raise WorkflowDepsLifecycleError("deps product has not completed construction")
        return self._product

    @property
    def resolved(self) -> bool:
        return self._product is not _UNRESOLVED

    @property
    def pins(self) -> int:
        return self._pins

    def pin(self) -> None:
        if self.retired or self.closed:
            raise WorkflowDepsLifecycleError("retired deps owners accept no new claimed attempts")
        self._pins += 1
        self._idle.clear()

    def release(self) -> None:
        if self._pins <= 0:
            raise WorkflowDepsLifecycleError("deps owner pin release without an acquire")
        self._pins -= 1
        if self._pins == 0:
            self._idle.set()

    def retire(self) -> None:
        self.retired = True

    async def acquire(self) -> object:
        if self.retired or self.closed:
            raise WorkflowDepsLifecycleError(
                "a retired/closed deps owner cannot construct a product"
            )
        self._assert_loop()
        if self._product is not _UNRESOLVED:
            return self._product
        flight = self._flight
        if flight is None:
            flight = asyncio.create_task(self._open(), name="workflow-deps-factory")
            self._flight = flight
        # Callers own their cancellation; the product belongs to this owner.
        await asyncio.shield(flight)
        return self.product

    async def _open(self) -> None:
        try:
            await self._open_user_code()
        except BaseException as exc:
            # A failed setup is retryable, never a cached failure, and every
            # failure has its own countable typed log before propagation.
            if self._flight is asyncio.current_task():
                self._flight = None
            logger.error(
                "workflow-deps-factory-failed",
                schema=self.context.schema,
                generation=self.context.generation,
                error_class=type(exc).__name__,
                error=str(exc),
                exc_info=True,
            )
            if isinstance(exc, SystemExit):
                raise _ProviderSystemExitError(exc) from exc
            raise

    async def _open_user_code(self) -> None:
        self.resolution_count += 1
        logger.info(
            "workflow-deps-factory-start",
            schema=self.context.schema,
            workflow_worker=str(self.context.worker_id),
            generation=self.context.generation,
            factory=getattr(self.factory, "__qualname__", type(self.factory).__qualname__),
        )
        target = self.factory
        raw = target(self.context)  # type: ignore[operator]  # callable checked at the owner boundary
        if inspect.isasyncgenfunction(target):
            generator = cast("AsyncGenerator[object, None]", raw)
            entered = await _enter_async_generator(generator, self.timeout)
        else:
            entered = await _enter_value(raw, self.timeout)
        self._product = entered.product
        self._close = entered.close  # installed before any validation can raise
        self._close_is_entry = entered.entry_context
        for owner, declared in self.expected:
            if deps_satisfied(entered.product, declared) is False:
                mismatch = WorkflowDepsProductTypeError(
                    f"deps_factory for {owner!r} produced "
                    f"{type(entered.product).__qualname__!r}, which does not satisfy "
                    f"declared deps type {getattr(declared, '__qualname__', declared)!r}"
                )
                # We are still on the factory's owned open task: awaiting
                # aclose() would create a close task waiting for THIS task.
                # Close the entered resource inline instead, exactly once.
                self.closed = True
                self.close_count += 1
                close = self._close
                if close is not None and (
                    id(entered.product) not in self.context._shared_identities  # pyright: ignore[reportPrivateUsage]  # shared parent ownership
                    or self._close_is_entry
                ):
                    try:
                        await close()
                    except BaseException as close_error:
                        logger.error(
                            "workflow-deps-mismatch-close-failed",
                            generation=self.context.generation,
                            error_class=type(close_error).__name__,
                            error=str(close_error),
                        )
                        if isinstance(close_error, KeyboardInterrupt):
                            raise
                self._flight = None
                raise mismatch
        logger.info(
            "workflow-deps-factory-resolved",
            schema=self.context.schema,
            workflow_worker=str(self.context.worker_id),
            generation=self.context.generation,
            resolutions=self.resolution_count,
        )

    async def aclose(self) -> None:
        """Owned, shielded, bounded close; late products still become owned."""
        self._assert_loop()
        self.retired = True
        close_flight = self._close_flight
        if close_flight is None:
            close_flight = asyncio.create_task(self._close_owned(), name="workflow-deps-close")
            self._close_flight = close_flight
        await asyncio.shield(close_flight)

    async def _close_owned(self) -> None:
        pending_cancel: BaseException | None = None
        flight = self._flight
        if flight is not None and not flight.done() and flight is not asyncio.current_task():
            try:
                # Factory setup has its own bound. Cancellation of shutdown's
                # waiter cannot truncate construction or orphan a late product.
                await asyncio.shield(flight)
            except asyncio.CancelledError as exc:
                pending_cancel = exc
            except BaseException as exc:
                logger.error(
                    "workflow-deps-factory-failed",
                    schema=self.context.schema,
                    generation=self.context.generation,
                    error_class=type(exc).__name__,
                    error=str(exc),
                )
                if isinstance(exc, KeyboardInterrupt):
                    raise
        if self._product is not _UNRESOLVED and not self.closed:
            self.closed = True
            self.close_count += 1
            close = self._close
            if close is not None and id(self._product) not in self.context._shared_identities:  # pyright: ignore[reportPrivateUsage]  # the context and owner jointly enforce parent-provider ownership
                try:
                    await close()
                except BaseException as exc:
                    logger.error(
                        "workflow-deps-close-failed",
                        schema=self.context.schema,
                        generation=self.context.generation,
                        error_class=type(exc).__name__,
                        error=str(exc),
                        exc_info=True,
                    )
                    if isinstance(exc, asyncio.CancelledError):
                        pending_cancel = exc
                    elif isinstance(exc, SystemExit):
                        raise _ProviderSystemExitError(exc) from exc
                    elif isinstance(exc, KeyboardInterrupt):
                        raise
            elif close is not None and self._close_is_entry:
                # A product that was itself entered by this factory still owns
                # its entry context; only direct parent-provider aclose is skipped.
                await close()
            logger.info(
                "workflow-deps-closed",
                schema=self.context.schema,
                generation=self.context.generation,
                closes=self.close_count,
            )
        if pending_cancel is not None:
            raise pending_cancel

    def _assert_loop(self) -> None:
        if asyncio.get_running_loop() is not self.loop:
            raise WorkflowDepsLifecycleError(
                "a workflow deps owner may only be used/closed by its creating loop"
            )


_UNRESOLVED = object()
