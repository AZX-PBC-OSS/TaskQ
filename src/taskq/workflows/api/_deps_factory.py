"""The deps seam's FACTORY arm — the lazy half of the body dependency door.

A static ``deps=`` instance is bound at declaration time and lives for the
life of the process. A factory is the honest door for every dependency that
cannot exist before the runtime loop does (an ``asyncpg`` pool, a loop-bound
HTTP client) or that needs an asynchronous release after the drive: it is
invoked LAZILY, on the running loop, at the first body execution.

TWO OBJECTS, one law:

* :class:`DepsFactory` is the DECLARATION — carried by the compile and
  read by every door. It owns the callable, the optional teardown
  callback, and the facts the build can prove WITHOUT invoking it: the
  product type E12 validates the bodies' declarations against, and whether
  the factory is statically a context-manager factory (whose exit owns its
  own teardown).
* :class:`DepsResolver` is one OWNER'S resolution state (one per runner; a
  worker-scoped registry composes the same object): lazy, single
  initialization under an :class:`asyncio.Lock`, and at most one teardown
  attempt.

THE SHAPES, one precedence — every resolved instance has EXACTLY ONE
teardown owner, and undergoes at most one teardown attempt:

1. an async context manager / async generator factory — its ``__aexit__`` /
   generator finalisation is the sole owner (it closes what it yielded, so
   pairing it with an explicit teardown is the refused dual ownership);
2. a plain sync/async factory with an explicit teardown callback — the
   callback is the sole owner even when the produced object also exposes
   ``aclose()``;
3. a plain factory with no callback — ``aclose()`` once when the produced
   object has one, otherwise teardown is a no-op.

The factory is NEVER invoked at declaration, compile, or validation time:
the product-type read is annotations only, and an unresolvable or
non-class annotation SKIPS the E12 check (the zero-false-positive doctrine
— a guess is never rejected).
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
)
from typing import Any, Protocol, Self, cast, get_args, get_origin

from taskq.workflows.api._hints import body_hints

__all__ = ["DepsFactory", "DepsResolution", "DepsResolver", "deps_factory_declaration"]

#: One resolution's release: the single teardown attempt the owner may make.
#: ``object`` is the honest return width — an async context manager's exit
#: answers ``bool | None`` and ``aclose()`` answers ``None``.
Release = Callable[[], Awaitable[object]]


class DepsFactory:
    """One ``deps_factory=`` declaration (see the module docstring)."""

    def __init__(
        self,
        factory: Callable[..., object],
        *,
        teardown: Callable[[object], object] | None = None,
    ) -> None:
        if not callable(factory):
            raise TypeError(
                "deps_factory= must be callable — a sync callable, an async callable, "
                "an async context manager factory, or an async generator function; "
                f"got {type(factory).__name__!r}"
            )
        if teardown is not None and not callable(teardown):
            raise TypeError(
                "deps_teardown= must be callable — it receives the resolved instance "
                f"and releases it (sync or async); got {type(teardown).__name__!r}"
            )
        # An async generator factory (a bare ``async def`` that yields, or an
        # ``@asynccontextmanager`` wrapper — ``functools.wraps`` keeps the inner
        # generator reachable through ``__wrapped__``) closes what it yields:
        # it is the resource's only teardown owner.
        if _is_async_generator_factory(factory) and teardown is not None:
            raise ValueError(
                "deps_factory= is an async context manager (or async generator) "
                "factory and deps_teardown= is also given — the factory's exit is the "
                "resource's ONE teardown owner (it closes what it yielded); drop "
                "deps_teardown=, or make the factory return a plain resource"
            )
        self.teardown = teardown
        #: The factory's product type, read from its return annotation
        #: (``() -> T``, ``() -> Awaitable[T]``, ``() -> AsyncContextManager[T]``,
        #: ``-> AsyncIterator[T]`` / ``AsyncGenerator[T, None]``); ``None`` when
        #: the annotation is absent or not a class. E12 separately SKIPS
        #: unverifiable products, including non-runtime-checkable protocols.
        self.product_type: type | None = _product_type(factory)
        # A BARE async generator function is not yet a context manager: wrap it
        # so its yielded value is the instance and its finalisation the
        # teardown. An ``@asynccontextmanager`` factory already returns the
        # context manager itself — wrapping it again would demand an async
        # iterator from a call that produces one (the double-wrap refusal).
        self._invoke: Callable[[], object] = (
            contextlib.asynccontextmanager(
                cast("Callable[..., AsyncGenerator[object, None]]", factory)
            )
            if inspect.isasyncgenfunction(factory)
            else factory
        )

    def invoke(self) -> object:
        """Call the factory. Only a resolution calls this — never the
        declaration, the compile, or the validator."""
        return self._invoke()


class DepsResolution(Protocol):
    """The resolution state a runner drives, whichever owner minted it.

    :class:`DepsResolver` is the per-runner owner (the direct door mints
    and closes it). The worker-hosted door injects its worker-and-loop-scoped
    resolution instead (``taskq.workflows._worker_deps``), which composes
    the same resolver and owns the resolve/teardown telemetry; the runner
    reads only these three members, and it closes a resolution only when it
    minted that resolution itself.
    """

    @property
    def binding(self) -> DepsFactory:
        """The declaration this resolution resolves (E12's view of a
        resolution the runner did not mint)."""
        ...

    async def get(self, *, workflow: str | None = None) -> object:
        """The resolved instance, created on the running loop at first use.

        *workflow* identifies the resolving body to an owner's telemetry;
        local owners need no label and ignore it."""
        ...

    async def aclose(self) -> None:
        """The one teardown attempt (a no-op when nothing resolved)."""
        ...


class DepsResolver:
    """One owner's lazy resolution of a :class:`DepsFactory`."""

    def __init__(self, binding: DepsFactory) -> None:
        self._binding = binding
        self._lock = asyncio.Lock()
        self._instance: object | None = None
        self._release: Release | None = None
        self._resolved = False
        self._closed = False

    @property
    def binding(self) -> DepsFactory:
        """The declaration this resolution was built from."""
        return self._binding

    async def get(self, *, workflow: str | None = None) -> object:
        """The resolved instance, created on the RUNNING loop at the first
        claim and reused from then on.

        *workflow* is the shared protocol's telemetry hint; this local owner
        emits no workflow lifecycle telemetry and ignores it.

        Concurrent first claims serialize on the lock, so the factory runs
        exactly once. A failed initialization caches NOTHING: the factory's
        exception propagates unchanged (cause intact) and the next claim
        retries the factory — the lock is released by the ``async with``
        unwind, never wedged.
        """
        del workflow
        if self._closed:
            raise _closed_error()
        if self._resolved:
            return self._instance
        async with self._lock:
            if self._closed:
                raise _closed_error()
            if self._resolved:
                return self._instance
            instance, release = await self._acquire()
            if self._closed:
                # aclose() may have timed out waiting for this acquisition's
                # lock. The resource arrived after its owner closed: release it
                # here rather than handing a live dependency to a body whose
                # worker has already unwound its teardown stack.
                if release is not None:
                    await release()
                raise _closed_error()
            # Assign only after a fully successful acquisition: a factory that
            # raised leaves no half-state behind, and ``__aexit__`` is never
            # called for a context manager whose ``__aenter__`` failed.
            self._instance, self._release, self._resolved = instance, release, True
            return instance

    async def aclose(self) -> None:
        """The ONE teardown attempt (a no-op when nothing resolved, and at
        most one attempt however often it is called)."""
        self._closed = True
        async with self._lock:
            release, self._release = self._release, None
            self._instance, self._resolved = None, False
            if release is not None:
                await release()

    async def _acquire(self) -> tuple[object, Release | None]:
        produced = self._binding.invoke()
        if inspect.isawaitable(produced):
            produced = await produced
        enter = getattr(produced, "__aenter__", None)
        exit_ = getattr(produced, "__aexit__", None)
        if callable(enter) and callable(exit_):
            # Shape 1: the produced object IS an async context manager (a sync
            # factory may return one). Its exit is the sole teardown owner.
            if self._binding.teardown is not None:
                raise ValueError(
                    "deps_factory= produced an async context manager but "
                    "deps_teardown= is also given — the context manager's exit is the "
                    "resource's ONE teardown owner (it closes what it yielded); drop "
                    "deps_teardown=, or return a plain resource from the factory"
                )
            instance = await cast("Callable[[], Awaitable[object]]", enter)()
            return instance, _as_release(
                functools.partial(cast("Callable[..., Awaitable[object]]", exit_), None, None, None)
            )
        return produced, self._plain_release(produced)

    def _plain_release(self, instance: object) -> Release | None:
        """Shapes 2 and 3: the explicit callback outranks ``aclose()``."""
        teardown = self._binding.teardown
        if teardown is not None:
            return _as_release(functools.partial(teardown, instance))
        aclose = getattr(instance, "aclose", None)
        if callable(aclose):
            return _as_release(cast("Callable[[], object]", aclose))
        return None


def deps_factory_declaration(
    *,
    deps: object | None,
    deps_factory: Callable[..., object] | None,
    deps_teardown: Callable[[object], object] | None,
) -> DepsFactory | None:
    """One door's declaration check: a static instance and a factory are
    mutually exclusive, and a teardown callback only makes sense beside a
    factory. Returns the binding to carry (``None`` when no factory was
    declared)."""
    if deps is not None and deps_factory is not None:
        raise ValueError(
            "deps= and deps_factory= are both given — the deps seam binds ONE "
            "instance: a static instance (deps=) or one created lazily on the "
            "running loop (deps_factory=), never both"
        )
    if deps_teardown is not None and deps_factory is None:
        raise ValueError(
            "deps_teardown= requires deps_factory= — a static deps= instance is not "
            "owned by the seam (release it where it was created); the callback "
            "releases a factory-created resource"
        )
    if deps_factory is None:
        return None
    return DepsFactory(deps_factory, teardown=deps_teardown)


def _as_release(call: Callable[[], object]) -> Release:
    """Normalise a sync or async release callable to one awaitable shape —
    the caller waits the single teardown attempt either way."""

    async def release() -> object:
        result = call()
        if inspect.isawaitable(result):
            return await result
        return result

    return release


def _closed_error() -> RuntimeError:
    return RuntimeError(
        "the deps owner is closed — its factory-created instance was already torn "
        "down; resolve deps from a live runner"
    )


def _is_async_generator_factory(factory: Callable[..., object]) -> bool:
    return inspect.isasyncgenfunction(inspect.unwrap(factory))


def _product_type(factory: Callable[..., object]) -> type | None:
    """The factory's product type from its return annotation, or ``None``.

    A class factory produces itself. A function's annotation is read through
    ``body_hints`` (the compile's one annotation-resolution seam — module
    globals, ``from __future__ import annotations`` strings, and the
    closure), unwrapped so an ``@asynccontextmanager`` decorator's
    ``functools.wraps`` copy is read from the function whose annotations
    actually resolve."""
    unwrapped = inspect.unwrap(factory)
    if isinstance(unwrapped, type):
        return _product_of(unwrapped)
    declared = body_hints(cast("Callable[..., object]", unwrapped)).get("return")
    if declared is None:
        return None
    return _product_of(declared)


def _product_of(annotation: object, *, enter_context_manager: bool = True) -> type | None:
    """The instance type behind a factory's return annotation: awaitables,
    async context managers, and async iterators/generators are unwrapped to
    their product; anything not a class (``Any``, ``object``, a union, a
    TypeVar, a generic alias) is UNVERIFIABLE and answers ``None`` — the
    zero-false-positive doctrine. E12 separately treats a protocol that is
    not runtime-checkable as unverifiable.

    A concrete async CM class names the wrapper, not the injected product;
    its ``__aenter__`` annotation names the latter. ``enter_context_manager``
    is false while reading that enter result: a CM yielded by another CM is
    itself the body dependency, not another layer the resolver enters."""
    origin = get_origin(annotation)
    if origin is None:
        if annotation is Any or annotation is object or not isinstance(annotation, type):
            return None
        product = cast("type[object]", annotation)
        enter = getattr(product, "__aenter__", None)
        exit_ = getattr(product, "__aexit__", None)
        if enter_context_manager and callable(enter) and callable(exit_):
            entered = body_hints(enter).get("return")
            if entered is Self:
                return product
            return (
                _product_of(entered, enter_context_manager=False) if entered is not None else None
            )
        return product
    args = get_args(annotation)
    if not args:
        return None
    if origin is contextlib.AbstractAsyncContextManager:
        return _product_of(args[0], enter_context_manager=False) if enter_context_manager else None
    try:
        if issubclass(origin, Coroutine):
            return _product_of(args[-1], enter_context_manager=enter_context_manager)
        if issubclass(origin, Awaitable):
            return _product_of(args[0], enter_context_manager=enter_context_manager)
        if issubclass(origin, (AsyncIterator, AsyncGenerator)):
            return (
                _product_of(args[0], enter_context_manager=False) if enter_context_manager else None
            )
    except TypeError:  # a non-class origin — unverifiable
        return None
    return None
