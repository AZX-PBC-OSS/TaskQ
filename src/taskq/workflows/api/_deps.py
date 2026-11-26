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

The BINDING is either a static instance or a factory invoked lazily on the
running loop (``deps_factory=`` — the declaration, the lifecycle shapes and
the resolution state live in ``_deps_factory``). E12 reads the factory's
PRODUCT TYPE rather than an instance, so the check is
``deps_type_satisfied``'s here; the unverifiable skip is the same doctrine
on both arms.

LANDMINE (named, test-scope): the InMemoryBackend has no workflow
execution surface — the seam's test face is the REAL ``FlowRunner`` (the
deps pins carry the name, so a future in-memory workflow runner inherits
the law, not a second mechanism). That runner also inherits PG's
admission law: ``capped_running`` subtracts ``wf``'s live flow roots
since #747, while the twin's running count (``testing/_dispatch.py``)
cannot — ``JobRow`` carries no ``step_key``; the twin's count is the
divergence, not the model.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

__all__ = ["deps_param_declared", "deps_satisfied", "deps_type_satisfied"]


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


def deps_type_satisfied(produced: object, declared: object) -> bool | None:
    """The FACTORY arm's satisfaction check: the type the factory's return
    annotation produces against the body's DECLARED deps type — the same
    verdict shape ``deps_satisfied`` gives the static binding
    (``True``/``False`` is the finding, ``None`` is UNVERIFIABLE and the
    zero-false-positive doctrine SKIPS it). A non-class on either side
    (``Any`` / ``object`` / a generic alias / an unannotated factory) or a
    non-runtime-checkable protocol is unverifiable; a factory product whose
    TYPE does not satisfy the declaration is E12's mismatch."""
    if (
        declared is Any
        or declared is object
        or not isinstance(declared, type)
        or (
            getattr(declared, "_is_protocol", False)
            and not getattr(declared, "_is_runtime_protocol", False)
        )
    ):
        return None
    if (
        produced is Any
        or produced is object
        or not isinstance(produced, type)
        or (
            getattr(produced, "_is_protocol", False)
            and not getattr(produced, "_is_runtime_protocol", False)
        )
    ):
        return None
    try:
        return issubclass(produced, declared)
    except TypeError:  # a runtime protocol with non-method members — unverifiable
        return None


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
