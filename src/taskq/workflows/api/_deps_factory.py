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
  initialization under an :class:`asyncio.Lock`, at most one teardown
  attempt, and — when the declaration names a GENERATION source — the
  rotation of that instance when the generation advances (see below).

GENERATIONS. A factory bound to changing configuration (a settings reload, a
rotated downstream client) declares a generation source. The owner then holds
one :class:`_Generation` per generation: :meth:`DepsResolver.rotate` reads the
declaration's current generation, swaps the owner to a fresh state BEFORE
draining, waits (bounded) for the superseded generation's in-flight holders to
leave, and only then releases the superseded instance — exactly once. The swap
first is what makes a claim arriving during the rotation get the NEW instance
rather than racing the teardown, and the drain is what keeps a body already
running against the old instance from having its resource closed underneath it.
A reload that finds the generation unchanged still drains the live one: it is
the caller's settle point before it swaps whatever those claims were admitted
with. A declaration with no generation source never rotates; its instance lives
for the owner's lifetime. The WORKER POOL a claim executes on is a different
lifetime, owned by the worker's reload gate (``taskq.worker.deps.ReloadGate``),
not by this module.

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
import dataclasses
import functools
import inspect
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
)
from typing import Any, Final, Protocol, Self, cast, get_args, get_origin

from taskq._close import CLOSE_TIMEOUT_SECS
from taskq.workflows.api._hints import body_hints

__all__ = [
    "INITIAL_DEPS_GENERATION",
    "DepsFactory",
    "DepsRelease",
    "DepsResolution",
    "DepsResolver",
    "DepsRotation",
    "Generation",
    "deps_factory_declaration",
]

#: A factory's generation label: whatever the declaration's source answers, as
#: long as it changes when the factory's inputs do (a settings version, a
#: credential epoch, a rotation counter). Carried verbatim into the lifecycle
#: telemetry, so it stays log- and attribute-safe (``int`` / ``str``).
type Generation = int | str

#: The generation a binding resolves at before any rotation. A declaration with
#: no generation source reports this value forever and never rotates.
INITIAL_DEPS_GENERATION: Final[int] = 0

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
        generation: Callable[[], Generation] | None = None,
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
        if generation is not None and not callable(generation):
            raise TypeError(
                "deps_generation= must be callable — it answers the factory's current "
                f"generation when a reload asks; got {type(generation).__name__!r}"
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
        #: The factory's generation source, read at every reload (never at
        #: declaration or compile time); ``None`` = the binding never rotates.
        self.generation_source = generation
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

    def current_generation(self) -> Generation:
        """The generation this binding declares NOW — read by a rotation, the
        only moment the source is consulted.

        A source that raises propagates: the reload that asked owns the
        disposition (its drain is reported and the binding keeps serving its
        current instance), and a guess at the generation would silently keep a
        stale instance alive."""
        source = self.generation_source
        return INITIAL_DEPS_GENERATION if source is None else _generation_of(source)


@dataclasses.dataclass(frozen=True)
class DepsRotation:
    """What one generation rotation did — the doors' telemetry input.

    ``generation`` is the generation that is CURRENT after the call (the same
    value as ``previous_generation`` when nothing rotated). ``drained`` is
    ``False`` when the drained generation's in-flight holders outlived the
    caller's bound (the superseded generation on a rotation, the live one on
    an unchanged-generation reload): a rotation's release still ran (a stuck
    body must not keep old-credential sessions open indefinitely) and the
    stuck body's later unwind is the ordinary failure path. ``error`` carries
    the superseded
    release's failure (or a generation source that raised), reported rather
    than raised so the rotation always completes and the caller names the cause
    in its own telemetry.
    """

    rotated: bool
    generation: Generation
    previous_generation: Generation
    had_instance: bool
    drained: bool
    error: BaseException | None = None
    #: The physical generation entry, for a telemetry owner deduplicating an
    #: overlapping rotation/shutdown. Private, keyword-only, and excluded from
    #: repr/equality so the public result's positional contract is unchanged.
    _entry: object | None = dataclasses.field(default=None, repr=False, compare=False, kw_only=True)


@dataclasses.dataclass(frozen=True)
class DepsRelease:
    """One instance's teardown attempt — its generation and its own outcome.

    ``had_instance`` says whether the generation ever minted an instance (a
    close that only waited out a lock is not a release); ``error`` is the
    failure that attempt hit (``None`` = released cleanly), so a multi-
    generation shutdown reports each generation's actual outcome instead of
    projecting one aggregate verdict over all of them.
    """

    generation: Generation
    had_instance: bool
    error: BaseException | None = None
    #: The physical generation entry; labels alone are not unique when a
    #: declaration cycles back to an earlier generation value. Private,
    #: keyword-only, and excluded from repr/equality.
    _entry: object | None = dataclasses.field(default=None, repr=False, compare=False, kw_only=True)


class DepsResolution(Protocol):
    """The resolution state a runner drives, whichever owner minted it.

    :class:`DepsResolver` is the per-runner owner (the direct door mints
    and closes it). The worker-hosted door injects its worker-and-loop-scoped
    resolution instead (``taskq.workflows._worker_deps``), which composes
    the same resolver and owns the resolve/teardown telemetry; the runner
    reads only these members, and it closes (or rotates) a resolution only
    when it minted that resolution itself.
    """

    @property
    def binding(self) -> DepsFactory:
        """The declaration this resolution resolves (E12's view of a
        resolution the runner did not mint)."""
        ...

    @property
    def generation(self) -> Generation:
        """The generation the current instance was minted for."""
        ...

    def body(
        self, *, workflow: str | None = None
    ) -> contextlib.AbstractAsyncContextManager[object]:
        """The current instance, LEASED for one body invocation: the
        generation's in-flight count is held for as long as the context is
        entered, so a rotation's drain sees this body and waits for it.

        *workflow* identifies the resolving body to an owner's telemetry;
        local owners need no label and ignore it."""
        ...

    def hold(self) -> contextlib.AbstractAsyncContextManager[None]:
        """Hold the CURRENT generation for a whole claim (a body plus the
        finalize that follows it) WITHOUT resolving anything: a rotation's
        drain then waits for the claim to finish before releasing the
        generation's instance, so neither the body nor its finalize runs
        against a released resource."""
        ...

    async def rotate(self, *, drain_timeout: float) -> DepsRotation:
        """Advance to the binding's current generation: swap first, drain the
        superseded generation's holders within *drain_timeout*, then release the
        superseded instance once. An unchanged generation drains the live one
        (``rotated=False``) — a reload's call, not a rotation."""
        ...

    async def aclose(self) -> object:
        """The one teardown attempt per instance this owner still holds (a
        no-op when nothing resolved). A :class:`DepsResolver` attempts every
        remaining generation and re-raises the first failure; a telemetry-
        owning wrapper reports each outcome itself. The runner closes a
        resolution only when it minted it and reads no answer."""
        ...


class _Generation:
    """ONE generation's resolution state: the lock, the instance, the single
    teardown attempt, and the bodies currently holding it (the drain's unit).

    Owned by a :class:`DepsResolver` for exactly as long as it is that
    resolver's current generation; a rotated-out generation is drained and
    released by the rotation that superseded it.
    """

    def __init__(self, binding: DepsFactory, generation: Generation) -> None:
        self.binding = binding
        self.generation = generation
        self._lock = asyncio.Lock()
        self._instance: object | None = None
        self._release: Release | None = None
        self._resolved = False
        self._closed = False
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def closed(self) -> bool:
        """Whether the one teardown attempt has begun (nothing resolves
        after it)."""
        return self._closed

    @property
    def resolved(self) -> bool:
        """Whether an instance was ever minted here (what a release reports)."""
        return self._resolved

    async def get(self) -> object:
        """The resolved instance, created on the RUNNING loop at the first
        claim and reused from then on.

        Concurrent first claims serialize on the lock, so the factory runs
        exactly once. A failed initialization caches NOTHING: the factory's
        exception propagates unchanged (cause intact) and the next claim
        retries the factory — the lock is released by the ``async with``
        unwind, never wedged.
        """
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
                    # The owner already spent its bounded close waiting for
                    # this acquisition. A late resource must not re-open an
                    # unbounded wait on the claim; keep the closed verdict
                    # primary if its one late release exceeds the same bound.
                    try:
                        await asyncio.wait_for(release(), timeout=CLOSE_TIMEOUT_SECS)
                    except TimeoutError as exc:
                        raise _closed_error() from exc
                raise _closed_error()
            # Assign only after a fully successful acquisition: a factory that
            # raised leaves no half-state behind, and ``__aexit__`` is never
            # called for a context manager whose ``__aenter__`` failed.
            self._instance, self._release, self._resolved = instance, release, True
            return instance

    def lease(self) -> None:
        """Enter one holder's (a body's or a claim's) hold on this
        generation's instance (synchronous on purpose: the caller pairs it with
        :meth:`unlease` and no rotation can interleave between a resolution and
        its hold)."""
        self._in_flight += 1
        self._idle.clear()

    def unlease(self) -> None:
        """End one holder's hold; the last one wakes the generation's drain."""
        self._in_flight -= 1
        if self._in_flight == 0:
            self._idle.set()

    async def drain(self, drain_timeout: float) -> bool:
        """Wait for this generation's in-flight holders, bounded; ``True`` when
        they all left inside *drain_timeout*."""
        if self._in_flight == 0:
            return True
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=drain_timeout)
        except TimeoutError:
            return False
        return True

    async def aclose(self, release_timeout: float) -> DepsRelease:
        """The ONE teardown attempt (a no-op when nothing resolved, and at
        most one attempt however often it is called).

        Bounded as a whole — the lock wait (a body still inside a hung factory
        holds it) and the release itself — and answering the attempt's own
        outcome rather than raising, so the caller reports this generation's
        release and every sibling's still runs.
        """
        self._closed = True
        had_instance = self._resolved
        try:
            await asyncio.wait_for(self._release_once(), timeout=release_timeout)
        except TimeoutError as exc:
            return DepsRelease(self.generation, had_instance, exc, _entry=self)
        except Exception as exc:
            return DepsRelease(self.generation, had_instance, exc, _entry=self)
        return DepsRelease(self.generation, had_instance, _entry=self)

    async def _release_once(self) -> None:
        async with self._lock:
            release, self._release = self._release, None
            self._instance, self._resolved = None, False
            if release is not None:
                await release()

    async def _acquire(self) -> tuple[object, Release | None]:
        produced = self.binding.invoke()
        if inspect.isawaitable(produced):
            produced = await produced
        if inspect.isasyncgen(produced):
            # A decorator can hide an async-generator function from
            # ``inspect.isasyncgenfunction`` while preserving its annotations.
            # The actual generator still has the context-manager lifecycle:
            # enter it for its yielded product instead of injecting the
            # generator itself. An @asynccontextmanager product is already a
            # context manager and does not take this arm.
            generator = cast("AsyncGenerator[object, None]", produced)
            produced = contextlib.asynccontextmanager(lambda: generator)()
        enter = getattr(produced, "__aenter__", None)
        exit_ = getattr(produced, "__aexit__", None)
        if callable(enter) and callable(exit_):
            # Shape 1: the produced object IS an async context manager (a sync
            # factory may return one). Its exit is the sole teardown owner.
            if self.binding.teardown is not None:
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
        teardown = self.binding.teardown
        if teardown is not None:
            return _as_release(functools.partial(teardown, instance))
        aclose = getattr(instance, "aclose", None)
        if callable(aclose):
            return _as_release(cast("Callable[[], object]", aclose))
        return None


class DepsResolver:
    """One owner's deps lifecycle of a :class:`DepsFactory`.

    Holds the CURRENT generation (:class:`_Generation`) and rotates it when the
    declaration's generation advances. Every claim's :meth:`hold` and
    :meth:`body` lease resolves against whichever generation is current at
    that moment; a claim whose resolution was interrupted by a rotation retries
    on the new generation rather than failing against a closed one, so a reload
    never turns a legitimate body into an error.
    """

    def __init__(self, binding: DepsFactory) -> None:
        self._binding = binding
        self._entry = _Generation(binding, INITIAL_DEPS_GENERATION)
        self._draining: list[_Generation] = []
        self._closed = False

    @property
    def binding(self) -> DepsFactory:
        """The declaration this resolution was built from."""
        return self._binding

    @property
    def generation(self) -> Generation:
        """The generation the current instance belongs to."""
        return self._entry.generation

    @contextlib.asynccontextmanager
    async def body(
        self,
        *,
        workflow: str | None = None,
        on_acquire: Callable[[Generation], None] | None = None,
    ) -> AsyncGenerator[object, None]:
        """The current generation's instance, held for one body invocation.

        The hold is taken in the same suspension-free step as the resolution,
        so a rotation either sees this body (and drains it) or answers it with
        the new generation — never a closed instance in between. The optional
        acquisition observer names the entry actually attempted, including a
        retry after rotation, for a telemetry-owning caller.
        """
        del workflow
        entry, instance = await self._acquire(on_acquire=on_acquire)
        entry.lease()
        try:
            yield instance
        finally:
            entry.unlease()

    @contextlib.asynccontextmanager
    async def hold(self) -> AsyncGenerator[None, None]:
        """Hold the current generation for a claim's whole lifetime, without
        resolving anything.

        The hold makes a rotation of the entry captured HERE wait for the
        claim's body and finalize before releasing that entry's instance. A
        body that resolves after the hold can move to a newer entry
        (``_acquire``), so this generation hold is not by itself a lease on
        the worker pool the claim uses.
        """
        entry = self._entry
        entry.lease()
        try:
            yield
        finally:
            entry.unlease()

    async def rotate(
        self, *, drain_timeout: float, release_timeout: float = CLOSE_TIMEOUT_SECS
    ) -> DepsRotation:
        """Advance to the binding's current generation, releasing the one this
        resolver served until now.

        Order is the contract: the swap happens FIRST (a claim arriving during
        the rotation gets the new generation, never the instance about to be
        released), then the superseded generation's in-flight holders get
        *drain_timeout* to leave, then its instance is released within
        *release_timeout* — one attempt, reported rather than raised. An
        unchanged generation still DRAINS the live one: a reload asks in-flight
        claims to settle before its caller swaps anything they were admitted
        with, and the generation holding the instance is what those claims are
        attached to.
        """
        current = self._entry
        if self._closed:
            # A reload racing this owner's teardown: nothing may resolve after
            # the one close, so no successor generation is installed.
            return DepsRotation(
                rotated=False,
                generation=current.generation,
                previous_generation=current.generation,
                had_instance=False,
                drained=True,
            )
        try:
            target = self._binding.current_generation()
        except Exception as exc:
            return DepsRotation(
                rotated=False,
                generation=current.generation,
                previous_generation=current.generation,
                had_instance=current.resolved,
                drained=True,
                error=exc,
            )
        if target == current.generation:
            return DepsRotation(
                rotated=False,
                generation=current.generation,
                previous_generation=current.generation,
                had_instance=current.resolved,
                drained=await current.drain(drain_timeout),
            )
        self._entry = _Generation(self._binding, target)
        self._draining.append(current)
        try:
            drained = await current.drain(drain_timeout)
            release = await current.aclose(release_timeout)
            error = release.error
        finally:
            # A cancelled rotation (a shutdown arriving mid-drain) leaves the
            # superseded generation listed so the owner's aclose() releases it.
            if current.closed and current in self._draining:
                self._draining.remove(current)
        return DepsRotation(
            rotated=True,
            generation=target,
            previous_generation=current.generation,
            had_instance=release.had_instance,
            drained=drained,
            error=error,
            _entry=current,
        )

    async def aclose(self) -> None:
        """The ONE teardown attempt for every instance this owner still holds:
        the current generation plus any superseded generation a cancelled
        rotation left unreleased.

        Each generation is attempted with its own bound, so one slow or failing
        release never skips a sibling; the first failure then re-raises after
        every attempt — the exit-stack contract a close that owns its resources
        keeps (the direct door's ``aclose``). A telemetry owner that must report
        one release per generation uses :meth:`close_generations` instead.
        """
        failure = next(
            (
                release.error
                for release in await self.close_generations()
                if release.error is not None
            ),
            None,
        )
        if failure is not None:
            raise failure

    async def close_generations(
        self, *, release_timeout: float = CLOSE_TIMEOUT_SECS
    ) -> list[DepsRelease]:
        """Every instance this owner still holds, each closed with its OWN
        bounded attempt and answered with its own outcome.

        One slow or failing release never skips a sibling and never mislabels
        one, and the answers come back in close order so a telemetry owner can
        report each generation's actual release, failure, or timeout instead of
        projecting one aggregate verdict over all of them.
        """
        self._closed = True
        releases: list[DepsRelease] = []
        for entry in (*list(self._draining), self._entry):
            releases.append(await entry.aclose(release_timeout))
            if entry in self._draining:
                # Remove only a completed close. Cancellation must leave every
                # unattempted sibling owned for a later close; an overlapping
                # rotation may already have removed this entry itself.
                self._draining.remove(entry)
        return releases

    async def _acquire(
        self, *, on_acquire: Callable[[Generation], None] | None = None
    ) -> tuple[_Generation, object]:
        """The current generation and its instance, retrying on the new
        generation when a rotation supersedes this claim's resolution."""
        while True:
            if self._closed:
                raise _closed_error()
            entry = self._entry
            if on_acquire is not None:
                on_acquire(entry.generation)
            try:
                return entry, await entry.get()
            except _DepsOwnerClosedError:
                if self._entry is entry:
                    raise


def deps_factory_declaration(
    *,
    deps: object | None,
    deps_factory: Callable[..., object] | None,
    deps_teardown: Callable[[object], object] | None,
    deps_generation: Callable[[], Generation] | None = None,
) -> DepsFactory | None:
    """One door's declaration check: a static instance and a factory are
    mutually exclusive, and the teardown callback and generation source only
    make sense beside a factory. Returns the binding to carry (``None`` when no
    factory was declared)."""
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
    if deps_generation is not None and deps_factory is None:
        raise ValueError(
            "deps_generation= requires deps_factory= — a generation rotates a "
            "factory-created instance; a static deps= instance is replaced where it "
            "was created, not by the seam"
        )
    if deps_factory is None:
        return None
    return DepsFactory(deps_factory, teardown=deps_teardown, generation=deps_generation)


def _as_release(call: Callable[[], object]) -> Release:
    """Normalise a sync or async release callable to one awaitable shape —
    the caller waits the single teardown attempt either way."""

    async def release() -> object:
        result = call()
        if inspect.isawaitable(result):
            return await result
        return result

    return release


class _DepsOwnerClosedError(RuntimeError):
    """A generation (or its owner) has begun its one teardown attempt: nothing
    resolves against it, and a claim that was mid-resolution when its generation
    rotated is retried on the successor instead."""


def _closed_error() -> _DepsOwnerClosedError:
    return _DepsOwnerClosedError(
        "the deps owner is closed — its factory-created instance was already torn "
        "down; resolve deps from a live runner"
    )


def _generation_of(source: Callable[[], Generation]) -> Generation:
    """Read a declaration's generation source. The value is carried into log
    events and metric attributes, so only the attribute-safe widths are
    accepted; anything else is a declaration bug named at its read site rather
    than silently coerced."""
    value = source()
    if not isinstance(value, int | str):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the annotation is the caller's promise, not a runtime fact — a settings-derived version can arrive as anything the source produced, and an unsupported width would only fail later inside the metric layer.
        raise TypeError(
            "deps_generation= must answer an int or a str (the generation rides log "
            f"events and metric attributes); it answered {type(value).__name__!r}"
        )
    return value


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
