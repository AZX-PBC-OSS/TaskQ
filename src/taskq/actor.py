"""Typed actor decorator and registration handle.

The :func:`actor` decorator is the user-facing entry point for
registering an async handler with the worker. It introspects the
handler's signature, validates it, and returns an :class:`ActorRef`
parameterized on the handler's payload (``P``) and return (``R``)
types.

Handler signatures follow the FastAPI principle — *declare what you
need*. The decorator accepts any of these shapes:

- ``async def fn(payload: P) -> R`` — payload only.
- ``def fn(payload: P) -> R`` — payload only (sync).
- ``async def fn(payload: P, ctx: JobContext[P]) -> R`` — payload + context.
- ``async def fn(payload: P, *, db: DbSession, http: HttpClient) -> R`` — payload + DI deps.
- ``async def fn(payload: P, ctx: JobContext[P], *, db: DbSession) -> R`` — all three.

The first parameter is always the validated payload (``P: BaseModel``).
The optional second positional parameter is the typed
:class:`JobContext`. Any further parameters are dependency-injection
requests: their names and annotations are captured on
:attr:`ActorRef.dependencies` and resolved by the worker's DI pass at
dispatch time. Whether deps arrive as keyword-only (``*, db: ...``) or
positional is up to the handler — the dispatcher always passes them
as keyword arguments.

Sync functions (plain ``def``) are accepted and dispatched to
:func:`asyncio.to_thread`, freeing the event loop for other work.
Sync actors must cooperate with cancellation by polling
:meth:`JobContext.should_abort`. LOOP-scoped DI dependencies (e.g.
``asyncpg.Connection``) are not thread-safe and should not be used
from sync actors.

The ref carries a :class:`pydantic.TypeAdapter` for ``R`` so the
:class:`~taskq.client._handle.JobHandle` can round-trip the actor's
return value through the JSONB ``result`` column without losing the
type parameter.

Decoration-time validation is strict: the decorator validates annotations
on payload, ctx, and DI parameters. Sync (``def``) and async (``async def``)
handlers are both accepted.
"""

import asyncio
import inspect
import typing
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, get_type_hints, overload

import structlog
from pydantic import BaseModel, TypeAdapter

from taskq.backend._protocol import JobStatus
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy

if TYPE_CHECKING:
    from taskq.context import JobContext

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

__all__ = ["ActorFn", "ActorFnWithCtx", "ActorHandler", "ActorRef", "actor"]


type ActorFn[P_: BaseModel, R_: (BaseModel | None)] = Callable[[P_], Awaitable[R_]]
"""Actor handler that takes only a payload.

The dispatcher injects nothing beyond the validated payload model.
Use this shape for actors that don't need cancellation cooperation,
attempt counters, or other context fields.
"""


type ActorFnWithCtx[P_: BaseModel, R_: (BaseModel | None)] = Callable[
    [P_, JobContext[P_]],
    Awaitable[R_],
]
"""Actor handler that takes a payload and a typed :class:`JobContext`.

Declare ``ctx: JobContext[YourPayload]`` as the second parameter to
opt into context injection. The dispatcher constructs the context per
attempt, populates it with the validated payload and a fresh
:class:`asyncio.Event` for cooperative cancellation, then passes it
to the handler. Handlers that don't declare ``ctx`` skip this work.
"""


class ActorHandler[P_: BaseModel, R_: BaseModel | None](Protocol):
    """Most-general actor signature: payload first, then ctx and/or DI deps.

    Pyright infers ``P_`` from the first positional parameter (the
    payload model) and ``R_`` from the awaited return type, regardless
    of how many additional ``ctx`` / DI parameters the handler
    declares. This lets the :func:`actor` decorator preserve full
    payload-and-result inference for FastAPI-style handlers like::

        async def my_actor(
            payload: OrderPayload,
            ctx: JobContext[OrderPayload],
            *,
            db: DbSession,
            http: HttpClient,
        ) -> OrderResult: ...

    Without ``ActorHandler``, callers would have to choose between
    :data:`ActorFn` (payload only) and :data:`ActorFnWithCtx` (payload
    + ctx), neither of which describes the DI case.
    """

    async def __call__(self, payload: P_, /, *args: object, **kwargs: object) -> R_: ...


class ActorRef[P: BaseModel, R: BaseModel | None]:
    """Typed reference to a registered actor.

    Created by the :func:`actor` decorator. Not callable directly via
    the queue path — enqueue jobs by passing this ref to
    :meth:`JobsClient.enqueue(ref, payload) <taskq.client.JobsClient.enqueue>`.
    Direct in-process invocation (``await my_actor(payload, ...)``) is
    available for tests and simulators.

    The two type parameters carry the actor's payload and result types
    end-to-end:

    - ``payload_type`` re-validates raw dispatch-time payloads back to
      ``P`` (via :meth:`pydantic.BaseModel.model_validate`).
    - ``result_adapter`` round-trips actor returns through the JSONB
      ``result`` column (``dump_python(mode="json")`` on the worker
      side, ``validate_python`` on the client side).

    Attributes:
        max_concurrent: Fleet-wide concurrency cap for this actor.
            ``None`` means unbounded — matches the
            ``actor_config.max_concurrent IS NULL`` semantics in the
            dispatch CTE. Allowed values are ``None`` or ``int >= 0``.
            A value of ``0`` means no jobs may run for this actor
            (useful for emergency drain scenarios).

            max_concurrent may transiently exceed configured value by
            up to (num_active_producers - 1) * max_concurrent per
            actor under heavy contention (or (num_producers - 1) *
            limit_n when limit_n < max_concurrent). For strict
            correctness, use ConcurrencyReservation.

        metadata: Arbitrary key-value metadata stored in
            ``actor_config.metadata`` (``jsonb NOT NULL``). Must be a
            plain ``dict[str, object]`` — mapping proxies and
            frozendicts are rejected at decoration time to avoid
            surprises at JSONB serialization time.

    Both are stored as instance fields rather than class metadata so
    pyright can infer ``P`` and ``R`` from a constructor call without
    relying on phantom-type tricks.

    ``wants_ctx`` records whether the handler declared a
    :class:`JobContext` parameter; the dispatcher passes ``ctx`` only
    when set. ``dependencies`` maps each DI parameter name to its
    annotated type — the worker's DI pass resolves these at dispatch
    time and passes them as keyword arguments to the handler. The DI
    resolver itself is an erasure boundary (see
    the resolver operates on the registered provider graph at runtime): user-declared annotations on
    DI parameters are captured here as ``type[object]`` because the
    resolver operates on the registered provider graph at runtime.
    """

    __slots__ = (
        "_fn",
        "dependencies",
        "is_sync",
        "max_concurrent",
        "max_pending",
        "metadata",
        "name",
        "non_retryable_exceptions",
        "on_retry_exhausted",
        "on_retry_exhausted_timeout",
        "on_success",
        "on_success_timeout",
        "payload_type",
        "priority",
        "queue",
        "rate_limits",
        "reservations",
        "result_adapter",
        "result_ttl",
        "retry",
        "retry_classifier",
        "singleton",
        "start_to_close",
        "unique_for",
        "unique_states",
        "wants_ctx",
    )

    def __init__(
        self,
        *,
        name: str,
        queue: str,
        fn: Callable[..., object],
        is_sync: bool = False,
        wants_ctx: bool,
        dependencies: dict[str, type[object]],
        payload_type: type[P],
        result_adapter: TypeAdapter[R],
        retry: RetryPolicy,
        result_ttl: timedelta | None,
        singleton: bool = False,
        max_concurrent: int | None = None,
        max_pending: int | None = None,
        metadata: dict[str, object] | None = None,
        unique_for: timedelta | None = None,
        unique_states: tuple[JobStatus, ...] = ("pending", "scheduled", "running"),
        start_to_close: timedelta | None = None,
        rate_limits: list[str] | None = None,
        reservations: list[str | KeyedReservationRef] | None = None,
        non_retryable_exceptions: tuple[type[BaseException], ...] = (),
        retry_classifier: RetryClassifierHook | None = None,
        on_retry_exhausted: OnRetryExhausted | None = None,
        on_retry_exhausted_timeout: float = 3.0,
        on_success: OnSuccess | None = None,
        on_success_timeout: float = 3.0,
        priority: int = 0,
    ) -> None:
        self.name = name
        self.queue = queue
        self.is_sync = is_sync
        self.wants_ctx = wants_ctx
        self.dependencies = dependencies
        self.payload_type = payload_type
        self.result_adapter = result_adapter
        self.retry = retry
        self.result_ttl = result_ttl
        self.singleton = singleton
        self.max_concurrent = max_concurrent
        self.max_pending = max_pending
        self.metadata = {} if metadata is None else metadata
        self.unique_for = unique_for
        self.unique_states = unique_states
        self.start_to_close = start_to_close
        self.rate_limits = [] if rate_limits is None else rate_limits
        self.reservations = [] if reservations is None else reservations
        self.non_retryable_exceptions = non_retryable_exceptions
        self.retry_classifier = retry_classifier
        self.on_retry_exhausted = on_retry_exhausted
        self.on_retry_exhausted_timeout = on_retry_exhausted_timeout
        self.on_success = on_success
        self.on_success_timeout = on_success_timeout
        self.priority = priority
        # Single storage slot. Call shape varies by handler — the
        # dispatcher (or :meth:`__call__`) routes based on
        # :attr:`wants_ctx`, :attr:`dependencies`, and :attr:`is_sync`.
        self._fn: Callable[..., object] = fn

    @property
    def fn(self) -> Callable[..., object]:
        """Return the underlying handler.

        May be sync (``def``) or async (``async def``). Direct
        invocation through :meth:`__call__` is the safer path because
        it handles both shapes and enforces the declared signature.
        """
        return self._fn

    @overload
    async def __call__(self, payload: P, /, **deps: object) -> R: ...
    @overload
    async def __call__(self, payload: P, ctx: "JobContext[P]", /, **deps: object) -> R: ...
    async def __call__(
        self,
        payload: P,
        ctx: "JobContext[P] | None" = None,
        /,
        **deps: object,
    ) -> R:
        """Direct invocation — bypasses enqueue, runs the handler in-process.

        Pass a :class:`JobContext` only when the registered handler
        declared one (when :attr:`wants_ctx` is ``True``); calling with
        a context against a no-ctx handler raises :class:`TypeError`,
        and calling without a context against a ctx handler also
        raises :class:`TypeError`.

        ``deps`` mirrors the keyword arguments the worker's
        dependency-injection pass supplies in production. Tests may
        pass them explicitly; the call site is responsible for
        matching the names recorded in :attr:`dependencies`. Missing
        dependencies surface as a runtime ``TypeError`` from Python's
        argument binding.

        Production callers go through :meth:`JobsClient.enqueue`.
        """
        if self.is_sync:
            actor_kwargs: dict[str, object] = {"payload": payload, **deps}
            if self.wants_ctx:
                if ctx is None:
                    raise TypeError(
                        f"actor {self.name!r} declares 'ctx: JobContext'; "
                        "supply a context to direct invocation"
                    )
                actor_kwargs["ctx"] = ctx
            elif ctx is not None:
                raise TypeError(
                    f"actor {self.name!r} does not declare a context parameter; "
                    "call it with payload only"
                )
            return await asyncio.to_thread(self._fn, **actor_kwargs)  # type: ignore[return-value]  # Why: asyncio.to_thread erases the return type to Any; the caller type-narrows through ActorRef[R].
        if self.wants_ctx:
            if ctx is None:
                raise TypeError(
                    f"actor {self.name!r} declares 'ctx: JobContext'; "
                    "supply a context to direct invocation"
                )
            return await self._fn(payload, ctx, **deps)  # type: ignore[return-value]  # Why: _fn is typed Callable[..., object] (sync or async); caller narrows through ActorRef[R].
        if ctx is not None:
            raise TypeError(
                f"actor {self.name!r} does not declare a context parameter; "
                "call it with payload only"
            )
        return await self._fn(payload, **deps)  # type: ignore[return-value]  # Why: _fn is typed Callable[..., object] (sync or async); caller narrows through ActorRef[R].


# ── Decorator ─────────────────────────────────────────────────────────


@overload
def actor[P: BaseModel, R: BaseModel | None](  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears in the return type of overloaded signatures.
    fn: Callable[..., object],
    /,
) -> ActorRef[P, R]: ...  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears once in the return type of this overload.
@overload
def actor[P: BaseModel, R: BaseModel | None](  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears in the return type of overloaded signatures.
    *,
    name: str | None = None,
    queue: str = "default",
    retry: RetryPolicy | None = None,
    result_ttl: timedelta | None = None,
    singleton: bool = False,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    metadata: dict[str, object] | None = None,
    unique_for: timedelta | None = None,
    unique_states: tuple[JobStatus, ...] = ("pending", "scheduled", "running"),
    start_to_close: timedelta | None = None,
    rate_limits: list[str] | None = None,
    reservations: list[str | KeyedReservationRef] | None = None,
    non_retryable_exceptions: tuple[type[BaseException], ...] = (),
    retry_classifier: RetryClassifierHook | None = None,
    on_retry_exhausted: OnRetryExhausted | None = None,
    on_retry_exhausted_timeout: float = 3.0,
    on_success: OnSuccess | None = None,
    on_success_timeout: float = 3.0,
    priority: int = 0,
) -> Callable[[Callable[..., object]], ActorRef[P, R]]: ...  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears once in the return type of this overload.
def actor[P: BaseModel, R: BaseModel | None](  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears in the return type of overloaded signatures.
    fn: Callable[..., object] | None = None,
    /,
    *,
    name: str | None = None,
    queue: str = "default",
    retry: RetryPolicy | None = None,
    result_ttl: timedelta | None = None,
    singleton: bool = False,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    metadata: dict[str, object] | None = None,
    unique_for: timedelta | None = None,
    unique_states: tuple[JobStatus, ...] = ("pending", "scheduled", "running"),
    start_to_close: timedelta | None = None,
    rate_limits: list[str] | None = None,
    reservations: list[str | KeyedReservationRef] | None = None,
    non_retryable_exceptions: tuple[type[BaseException], ...] = (),
    retry_classifier: RetryClassifierHook | None = None,
    on_retry_exhausted: OnRetryExhausted | None = None,
    on_retry_exhausted_timeout: float = 3.0,
    on_success: OnSuccess | None = None,
    on_success_timeout: float = 3.0,
    priority: int = 0,
) -> ActorRef[P, R] | Callable[[ActorHandler[P, R]], ActorRef[P, R]]:
    """Register an async handler as a typed :class:`ActorRef`.

    All shapes are supported — declare what you need::

        # Payload only.
        @actor
        async def my_actor(payload: MyPayload) -> MyResult:
            ...

        # Payload + typed context.
        @actor
        async def my_actor(payload: MyPayload, ctx: JobContext[MyPayload]) -> MyResult:
            ...

        # Payload + DI deps (FastAPI-style).
        @actor
        async def my_actor(
            payload: MyPayload,
            *,
            db: DbSession,
            http: HttpClient,
        ) -> MyResult:
            ...

        # Payload + ctx + DI deps.
        @actor(queue="priority")
        async def my_actor(
            payload: MyPayload,
            ctx: JobContext[MyPayload],
            *,
            db: DbSession,
        ) -> MyResult:
            ...

    Pyright infers ``P`` from the first positional parameter and ``R``
    from the return annotation, then propagates them through
    ``ActorRef[P, R]`` to :meth:`JobsClient.enqueue` and
    :class:`JobHandle[R]`.

    Args:
        max_concurrent: Fleet-wide concurrency cap for this actor.
            ``None`` means unbounded — matches the
            ``actor_config.max_concurrent IS NULL`` semantics in the
            dispatch CTE. Allowed values are ``None`` or ``int >= 0``.
            A value of ``0`` means no jobs may run for this actor
            (useful for emergency drain scenarios).

            max_concurrent may transiently exceed configured value by
            up to (num_active_producers - 1) * max_concurrent per
            actor under heavy contention (or (num_producers - 1) *
            limit_n when limit_n < max_concurrent). For strict
            correctness, use ConcurrencyReservation.

        max_pending: Queue-depth backpressure cap for this actor.
            ``None`` means unbounded — enqueue never rejects on capacity.
            A non-negative ``int`` limits the number of ``pending`` and
            ``scheduled`` jobs allowed before :meth:`JobsClient.enqueue`
            raises ``MaxPendingExceededError``. ``max_pending=0`` means
            never accept any jobs (every enqueue immediately rejects).
            Negative values raise ``ValueError`` at decoration time.

        singleton: Enforce at most one active job of this actor across the
            fleet (``False`` by default). When ``True``, the enqueue path
            injects ``metadata.singleton = true`` on the job row, which
            triggers the ``jobs_singleton_uniq`` partial unique index.
            Singleton enforcement is actor-scoped, not identity-scoped:
            different ``identity_key`` values for the same singleton actor
            are still blocked. ``scheduled`` is an active state for
            singleton enforcement — a snoozed singleton job blocks new
            enqueues until it terminates. For per-identity singleton
            semantics use ``max_concurrent=1`` with an ``identity`` key
            instead. The library reserves the ``metadata.singleton``
            JSONB key — callers MUST NOT set it manually.

        metadata: Arbitrary key-value metadata stored in
            ``actor_config.metadata`` (``jsonb NOT NULL``). Must be a
            plain ``dict[str, object]`` — mapping proxies and
            frozendicts are rejected at decoration time to avoid
            surprises at JSONB serialization time. Pass ``None`` to
            get an empty ``dict`` (the default).

        unique_states: The set of job statuses to consider "active" for
            ``unique_for`` deduplication. Defaults to
            ``("pending", "scheduled", "running")`` — terminal states
            (``succeeded``, ``failed``, ``cancelled``) are excluded so
            that a completed job does not block re-enqueue of the same
            identity. To include succeeded jobs, pass
            ``unique_states=("pending", "scheduled", "running",
            "succeeded")``. Misconfigured terminal states block
            re-enqueue after success (which is rarely intended).
    """

    def _wrap(handler: Callable[..., object]) -> ActorRef[P, R]:
        return _build_ref(
            handler,
            name=name,
            queue=queue,
            retry=retry if retry is not None else RetryPolicy(),
            result_ttl=result_ttl,
            singleton=singleton,
            max_concurrent=max_concurrent,
            max_pending=max_pending,
            metadata=metadata,
            unique_for=unique_for,
            unique_states=unique_states,
            start_to_close=start_to_close,
            rate_limits=rate_limits,
            reservations=reservations,
            non_retryable_exceptions=non_retryable_exceptions,
            retry_classifier=retry_classifier,
            on_retry_exhausted=on_retry_exhausted,
            on_retry_exhausted_timeout=on_retry_exhausted_timeout,
            on_success=on_success,
            on_success_timeout=on_success_timeout,
            priority=priority,
        )

    if fn is not None:
        return _wrap(fn)
    return _wrap


def _resolve_hints(fn: Callable[..., object]) -> dict[str, object]:
    """Resolve ``fn``'s type hints, falling back to raw annotations.

    :func:`typing.get_type_hints` raises :class:`NameError` when a
    forward reference cannot be resolved (a common case when a user
    defines an actor in module ``A`` whose payload model is defined
    in module ``B`` and ``B`` has not yet imported ``A``). The
    fallback to ``__annotations__`` returns the unresolved string
    annotations; downstream validation surfaces a clear error if a
    string slips through where a class was expected.
    """
    try:
        return get_type_hints(fn, include_extras=False)
    except NameError:
        return dict(getattr(fn, "__annotations__", {}))


def _origin_is_job_context(annotation: object) -> bool:
    """Return ``True`` if ``annotation`` resolves to ``JobContext[...]``."""
    from taskq.context import JobContext

    origin = typing.get_origin(annotation)
    if origin is JobContext:
        return True
    return annotation is JobContext


def _build_ref[P: BaseModel, R: BaseModel | None](  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears in the return type of this helper.
    fn: Callable[..., object],
    *,
    name: str | None,
    queue: str,
    retry: RetryPolicy,
    result_ttl: timedelta | None,
    singleton: bool,
    max_concurrent: int | None,
    max_pending: int | None = None,
    metadata: dict[str, object] | None,
    unique_for: timedelta | None = None,
    unique_states: tuple[JobStatus, ...] = ("pending", "scheduled", "running"),
    start_to_close: timedelta | None = None,
    rate_limits: list[str] | None = None,
    reservations: list[str | KeyedReservationRef] | None = None,
    non_retryable_exceptions: tuple[type[BaseException], ...] = (),
    retry_classifier: RetryClassifierHook | None = None,
    on_retry_exhausted: OnRetryExhausted | None = None,
    on_retry_exhausted_timeout: float = 3.0,
    on_success: OnSuccess | None = None,
    on_success_timeout: float = 3.0,
    priority: int = 0,
) -> ActorRef[P, R]:  # pyright: ignore[reportInvalidTypeVarUse]  # Why: TypeVars P, R are intentional for variance-free generics; each appears once in the return type of _build_ref.
    """Introspect ``fn``'s annotations and construct an :class:`ActorRef`.

    Accepts both sync (``def``) and async (``async def``) handlers.
    Sync handlers are stored with ``is_sync=True`` and dispatched via
    :func:`asyncio.to_thread` at runtime. All other decoration-time
    validations (payload type, annotations, result type) apply
    identically to both shapes.

    Strict decoration-time validation. All checks raise :class:`TypeError`
    with a message naming the offending handler so the failure mode at
    import time is unambiguous. The runtime checks here mirror the
    pyright contract enforced on call sites of :func:`actor`; they
    catch users who poison the type checker with ``Any`` or who use
    forward references that resolve to non-``BaseModel`` types at
    runtime.

    Parameters past the first are classified by annotation type rather
    than position: a ``JobContext[P]`` annotation (anywhere) becomes
    the context parameter; everything else becomes a DI request
    captured in :attr:`ActorRef.dependencies`.
    """
    is_sync = not inspect.iscoroutinefunction(fn)

    if metadata is not None and not isinstance(metadata, dict):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: runtime guard against callers that bypass the type checker.
        raise TypeError(
            f"actor handler {fn.__qualname__!r} metadata must be a dict or None; "
            f"got {type(metadata).__name__!r}.",
        )

    if max_concurrent is not None and (not isinstance(max_concurrent, int) or max_concurrent < 0):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: runtime guard against callers that bypass the type checker.
        raise ValueError(
            f"actor handler {fn.__qualname__!r} max_concurrent must be None "
            f"or a non-negative integer; got {max_concurrent!r}.",
        )

    if not isinstance(priority, int):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: runtime guard against callers that bypass the type checker.
        raise TypeError(
            f"actor handler {fn.__qualname__!r} priority must be an int; "
            f"got {type(priority).__name__!r}.",
        )
    if priority < -32768 or priority > 32767:
        raise ValueError(
            f"actor handler {fn.__qualname__!r} priority must fit "
            f"smallint range (-32768..32767); got {priority}.",
        )

    if max_pending is not None and (not isinstance(max_pending, int) or max_pending < 0):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: runtime guard against callers that bypass the type checker.
        raise ValueError(
            f"actor handler {fn.__qualname__!r} max_pending must be None "
            f"or a non-negative integer; got {max_pending!r}.",
        )

    if start_to_close is not None and start_to_close <= timedelta(0):
        raise ValueError(
            f"actor handler {fn.__qualname__!r} start_to_close must be > 0; "
            f"got {start_to_close!r}.",
        )

    if not isinstance(singleton, bool):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: runtime guard against callers that bypass the type checker.
        raise TypeError(
            f"actor handler {fn.__qualname__!r} singleton must be a bool; got {singleton!r}."
        )

    sig = inspect.signature(fn)
    hints = _resolve_hints(fn)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"actor handler {fn.__qualname__!r} must take at least a 'payload' parameter",
        )

    payload_param = params[0]
    if payload_param.name not in hints:
        raise TypeError(
            f"actor handler {fn.__qualname__!r} is missing an annotation "
            f"for its payload parameter {payload_param.name!r}",
        )
    if "return" not in hints:
        raise TypeError(
            f"actor handler {fn.__qualname__!r} is missing a return annotation; "
            "result type cannot be inferred for the JobHandle round-trip",
        )

    payload_annotation = hints[payload_param.name]
    if not (isinstance(payload_annotation, type) and issubclass(payload_annotation, BaseModel)):
        raise TypeError(
            f"actor handler {fn.__qualname__!r} payload parameter "
            f"{payload_param.name!r} must be annotated with a Pydantic "
            f"BaseModel subclass; got {payload_annotation!r}. "
            "Plain dicts, dataclasses, and TypedDicts are not supported "
            "",
        )
    payload_type: type[P] = typing.cast("type[P]", payload_annotation)

    # Walk remaining parameters: classify each as ctx or DI by annotation.
    wants_ctx = False
    ctx_param_name: str | None = None
    dependencies: dict[str, type[object]] = {}
    for param in params[1:]:
        if param.name not in hints:
            raise TypeError(
                f"actor handler {fn.__qualname__!r} parameter "
                f"{param.name!r} is missing a type annotation; the "
                "DI pass cannot resolve unannotated parameters",
            )
        annotation = hints[param.name]
        if _origin_is_job_context(annotation):
            if wants_ctx:
                raise TypeError(
                    f"actor handler {fn.__qualname__!r} declares more than "
                    "one JobContext parameter; only one is allowed.",
                )
            ctx_args = typing.get_args(annotation)
            if ctx_args and ctx_args[0] is not payload_type:
                raise TypeError(
                    f"actor handler {fn.__qualname__!r} declares "
                    f"payload type {payload_type.__name__!r} but parameter "
                    f"{param.name!r} is parameterized on {ctx_args[0]!r}; "
                    "the two payload types must match.",
                )
            wants_ctx = True
            ctx_param_name = param.name
            continue
        # DI dependency: annotation must resolve to a concrete type at
        # runtime so the resolver can build a provider graph.
        if not isinstance(annotation, type):
            raise TypeError(
                f"actor handler {fn.__qualname__!r} dependency parameter "
                f"{param.name!r} annotation must be a class for the DI "
                f"resolver; got {annotation!r}.",
            )
        dependencies[param.name] = annotation

    if wants_ctx and ctx_param_name is not None and ctx_param_name in dependencies:
        # Defensive: should be unreachable since the loop ``continue``s
        # on the ctx branch before adding to ``dependencies``.
        del dependencies[ctx_param_name]

    result_annotation = hints["return"]
    result_adapter: TypeAdapter[R] = TypeAdapter(result_annotation)

    actor_name = name or fn.__qualname__

    if retry.kind == "indefinite" and retry.time_budget is None:
        logger.warning(
            "actor-config-indefinite-no-budget",
            actor=actor_name,
            queue=queue,
        )

    if retry.kind != "indefinite" and retry.time_budget is not None:
        logger.warning(
            "actor-config-time-budget-ignored",
            actor=actor_name,
            queue=queue,
            retry_kind=retry.kind,
            time_budget=str(retry.time_budget),
        )

    if (
        retry.kind == "indefinite"
        and retry.time_budget is not None
        and retry.time_budget > timedelta(hours=24)
    ):
        logger.warning(
            "actor-config-indefinite-long-budget",
            actor=actor_name,
            queue=queue,
            time_budget=str(retry.time_budget),
        )

    return ActorRef(
        name=actor_name,
        queue=queue,
        fn=fn,
        is_sync=is_sync,
        wants_ctx=wants_ctx,
        dependencies=dependencies,
        payload_type=payload_type,
        result_adapter=result_adapter,
        retry=retry,
        result_ttl=result_ttl,
        singleton=singleton,
        max_concurrent=max_concurrent,
        max_pending=max_pending,
        metadata={} if metadata is None else metadata,
        unique_for=unique_for,
        unique_states=unique_states,
        start_to_close=start_to_close,
        rate_limits=[] if rate_limits is None else rate_limits,
        reservations=[] if reservations is None else reservations,
        non_retryable_exceptions=non_retryable_exceptions,
        retry_classifier=retry_classifier,
        on_retry_exhausted=on_retry_exhausted,
        on_retry_exhausted_timeout=on_retry_exhausted_timeout,
        on_success=on_success,
        on_success_timeout=on_success_timeout,
        priority=priority,
    )
