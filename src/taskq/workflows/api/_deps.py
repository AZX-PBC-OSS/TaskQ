"""The
body-side opt-in read, and the bound instance's satisfaction check.

The seam: a ``Deps`` dataclass declared PER APP, one instance bound at
the door (``WorkflowApp(deps=…)`` / ``FlowRunner(…, deps=…)``), bodies
opting in by declaring one parameter beyond ``ctx`` + the wired sources.
The runner injects the bound instance as the last positional argument —
the same arity discipline E10 owns, extended honestly: the extra
parameter is the deps contract (E12), never a wired source.

TWO READS, one SHAPE:

* the VALIDATOR reads the body's RESOLVED ANNOTATION HINTS (the same
  seam E5/E10 read — ``body_hints``): the declaration is what E12
  rejects, at build, with the types named;
* the RUNNER reads the body's RUNTIME SIGNATURE (the positional truth —
  an unresolvable-annotations body is exactly the shape E10/E12 skip,
  and the injection is positional-mechanical): one parameter beyond
  ``ctx`` + the wired sources, no ``*args`` absorption, and the bound
  instance rides in as the last argument.

Both reads answer the same question — "does this body declare the deps
shape?" — so a body the validator rejected never reaches a claim, and a
body the validator skipped (unresolvable annotations) either takes the
deps shape at runtime (the injection is correct) or fails arity at the
invocation (the ladder's discovery, the pre-existing class E10's
zero-false-positive doctrine already owns).

LANDMINE (named, test-scope): the InMemoryBackend has no workflow
execution surface — the seam's test face is the REAL ``FlowRunner`` (the
deps pins carry the name, so a future in-memory workflow runner inherits
the law, not a second mechanism).
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
)
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, get_args, get_origin

from taskq.workflows.api._hints import body_hints

if TYPE_CHECKING:
    from taskq.workflows.api._app import CompiledWorkflow

__all__ = [
    "FactoryProductType",
    "deps_bodies",
    "deps_contract_annotations",
    "deps_param_declared",
    "deps_satisfied",
    "factory_product_type",
    "workflow_has_runtime_deps",
]


def deps_param_declared(body: Callable[..., object], wired_count: int) -> bool:
    """Whether *body* declares the deps shape at runtime: exactly one
    positional parameter beyond ``ctx`` + the *wired_count* wired
    sources, and no ``*args`` absorption (a variadic body swallows the
    injection silently — never opted in). Un-inspectable bodies (builtins
    and partials raise) declare NOTHING — the injection stays off."""
    try:
        sig = inspect.signature(body)
    except (TypeError, ValueError):
        return False
    positional = 0
    for parameter in sig.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            return False
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional == wired_count + 2  # ctx + the wired sources + deps


def deps_satisfied(instance: object, declared: object) -> bool | None:
    """The bound instance's satisfaction of the body's DECLARED deps
    type. ``True``/``False`` is the verdict (``False`` is E12's
    type-mismatch finding); ``None`` is UNVERIFIABLE — a non-class
    annotation (``Any`` / ``object`` / a generic alias), or a class the
    runtime check cannot see (a non-runtime-checkable protocol) — and
    the zero-false-positive doctrine SKIPS it (a guess is never
    rejected)."""
    if declared is Any or declared is object or not isinstance(declared, type):
        return None
    try:
        return isinstance(instance, declared)
    except TypeError:  # a non-runtime-checkable protocol — unverifiable
        return None


@dataclass(frozen=True, slots=True)
class FactoryProductType:
    """The non-constructing read of a factory's declared runtime product.

    ``type_`` is the class to use in E12's ``issubclass`` verdict. ``None``
    means deliberately unverifiable; ``reason`` is the discrete diagnostic
    text rather than an escaped ``NameError``/``TypeError`` from annotation
    machinery.
    """

    type_: object | None
    reason: str | None = None


def _factory_callable(factory: object) -> object:
    """Find the annotation-carrying callable without calling it."""
    if isinstance(factory, functools.partial):
        return _factory_callable(factory.func)
    if inspect.isroutine(factory) or isinstance(factory, type):
        return factory
    call = inspect.getattr_static(type(factory), "__call__", None)
    return factory if call is None else call


def _annotations(factory: object) -> dict[str, object]:
    target = _factory_callable(factory)
    if not callable(target):
        return {}
    try:
        # body_hints owns the repository's best-effort namespace recovery,
        # including closure locals under ``from __future__ annotations``.
        return body_hints(target)
    except (AttributeError, NameError, TypeError, ValueError):
        # Kept as a second fence even though body_hints currently contains
        # these implementation exceptions itself.
        return {}


def _async_generator_factory(factory: object) -> bool:
    target = _factory_callable(factory)
    if not callable(target):
        return False
    if inspect.isasyncgenfunction(target):
        return True
    try:
        inner = inspect.unwrap(target)
    except (ValueError, TypeError):
        return False
    return inner is not target and inspect.isasyncgenfunction(inner)


def _unwrap_factory_annotation(factory: object, annotation: object) -> object:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Coroutine and len(args) == 3:
        return _unwrap_factory_annotation(factory, args[2])
    if origin is Awaitable and len(args) == 1:
        return _unwrap_factory_annotation(factory, args[0])
    if origin in (AbstractAsyncContextManager,) and len(args) >= 1:
        return args[0]
    # ``typing.AsyncContextManager`` and ``contextlib``'s alias differ in
    # spelling across supported Python releases; the ABC origin is stable.
    if origin is not None and getattr(origin, "__name__", "") == "AsyncContextManager" and args:
        return args[0]
    if (
        _async_generator_factory(factory)
        and origin in (AsyncGenerator, AsyncIterable, AsyncIterator)
        and args
    ):
        return args[0]
    return annotation


def factory_product_type(factory: object) -> FactoryProductType:
    """Resolve a plain/awaitable/ACM/async-generator factory product type.

    Callable instances, partials, functions and classes are covered. A class
    is itself the constructor product type, so its constructor is never
    invoked and its ``__init__`` return annotation is never misread.
    """
    constructor = _factory_callable(factory)
    if isinstance(constructor, type):
        return FactoryProductType(constructor)
    annotations = _annotations(factory)
    if "return" not in annotations:
        return FactoryProductType(
            None,
            f"{getattr(factory, '__qualname__', type(factory).__qualname__)!r} has no "
            "resolvable product return annotation",
        )
    product = _unwrap_factory_annotation(factory, annotations["return"])
    origin = get_origin(product)
    if origin in (AsyncGenerator, AsyncIterable, AsyncIterator) and not _async_generator_factory(
        factory
    ):
        # A sync wrapper can return any implementation of these interfaces.
        # Runtime ownership accepts and enters a real async generator, but the
        # annotation alone cannot prove the yielded product class. Refusing the
        # ABC as though it were the product contradicts that runtime behavior.
        return FactoryProductType(
            None,
            f"sync/wrapper async-generator annotation {product!r} does not prove "
            "the yielded product type; runtime resolution validates the concrete product",
        )
    # E12 needs only the runtime class object, never the generic interface's
    # unknown parameters. Wrapper annotations returned unverifiable above.
    candidate: object = cast("type[object]", origin) if isinstance(origin, type) else product
    if not isinstance(candidate, type):
        return FactoryProductType(
            None,
            f"factory product annotation {product!r} is not a runtime-checkable class",
        )
    return FactoryProductType(candidate)


def deps_bodies(compiled: CompiledWorkflow) -> list[tuple[str, Callable[..., object], int]]:
    """One body walk for E12 and the worker's runtime product check.

    Chain steps are already registered by ``_register_bodies``; including
    their exact existing declaration objects here adds no second runtime
    registration or dispatch mechanism. Future terminal/atomic bodies must
    extend this same walk with their owning P4/P5 fields.
    """
    from taskq.workflows.api._graph import route_child_key
    from taskq.workflows.chain import Chain

    checks: list[tuple[str, Callable[..., object], int]] = []
    for node in compiled.nodes.values():
        if node.body is not None:
            checks.append((node.key, node.body, len(node.args)))
        if node.map_arms is not None:
            for tag, arm in node.map_arms.items():
                checks.append((route_child_key(node.key, tag), arm.body, 1))
        if node.map_item is not None:
            checks.append((f"{node.key}.item", node.map_item, 1))
        if node.loop_body is not None:
            checks.append((node.key, node.loop_body, 1))
        if node.loop_spec is not None and node.loop_spec.escalation_body is not None:
            escalation = node.loop_spec.escalation_body
            if callable(escalation):
                checks.append((f"{node.key}.escalation", escalation, 1))
    for declaration in compiled.chains:
        chain = declaration
        if not isinstance(chain, Chain):  # pyright: ignore[reportUnnecessaryIsInstance]  # retain the existing hand-built-compile runtime guard
            continue
        for step_key, chain_step in chain.steps.items():
            body = chain_step.body
            if callable(body):
                checks.append((step_key, body, 1))
    from taskq.workflows._effects import collect_terminal_effects

    for step_key, registered in collect_terminal_effects(compiled).items():
        checks.append((step_key, registered.effect.body, 1))
    return checks


def deps_contract_annotations(compiled: CompiledWorkflow) -> tuple[tuple[str, object], ...]:
    """The declared product types the runtime factory must satisfy."""
    found: list[tuple[str, object]] = []
    for owner, body, wired in deps_bodies(compiled):
        hints = body_hints(body)
        if not hints:
            continue
        params = [key for key in hints if key not in ("return", "ctx")]
        if len(params) == wired + 1:
            found.append((owner, hints[params[-1]]))
    return tuple(found)


def workflow_has_runtime_deps(compiled: CompiledWorkflow) -> bool:
    """Whether any existing registered body takes the positional deps tail."""
    return any(deps_param_declared(body, wired) for _owner, body, wired in deps_bodies(compiled))
