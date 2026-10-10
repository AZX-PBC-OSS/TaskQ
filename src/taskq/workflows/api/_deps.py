"""THE DEPS SEAM'S CONTRACT HELPERS (the DI capability's one home): the
body-side opt-in read, and the bound instance's satisfaction check.

The seam: a ``Deps`` dataclass declared PER APP, ONE instance bound at
the door (``WorkflowApp(deps=…)`` / ``FlowRunner(…, deps=…)``), bodies
opting in by declaring ONE parameter beyond ``ctx`` + the wired sources.
The runner injects the bound instance as the LAST positional argument —
the same arity discipline E10 owns, extended honestly: the extra
parameter is the deps contract (E12), never a wired source.

TWO READS, ONE SHAPE:

* the VALIDATOR reads the body's RESOLVED ANNOTATION HINTS (the same
  seam E5/E10 read — ``body_hints``): the declaration is what E12
  convicts, at build, with the types named;
* the RUNNER reads the body's RUNTIME SIGNATURE (the positional truth —
  an unresolvable-annotations body is exactly the shape E10/E12 skip,
  and the injection is positional-mechanical): one parameter beyond
  ``ctx`` + the wired sources, no ``*args`` absorption, and the bound
  instance rides in as the last argument.

Both reads answer the SAME question — "does this body declare the deps
shape?" — so a body the validator convicted never reaches a claim, and a
body the validator skipped (unresolvable annotations) either takes the
deps shape at runtime (the injection is correct) or fails arity at the
invocation (the ladder's discovery, the pre-existing class E10's
zero-false-positive doctrine already owns).

THE LANDMINE (named, test-scope): the InMemoryBackend has NO workflow
execution surface — the seam's test face is the REAL ``FlowRunner`` (the
deps pins carry the name, so a future in-memory workflow runner inherits
the law, not a second mechanism).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

__all__ = ["deps_param_declared", "deps_satisfied"]


def deps_param_declared(body: Callable[..., object], wired_count: int) -> bool:
    """Whether *body* declares the deps shape at runtime: exactly ONE
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
    type-mismatch conviction); ``None`` is UNVERIFIABLE — a non-class
    annotation (``Any`` / ``object`` / a generic alias), or a class the
    runtime check cannot see (a non-runtime-checkable protocol) — and
    the zero-false-positive doctrine SKIPS it (a guess is never
    convicted)."""
    if declared is Any or declared is object or not isinstance(declared, type):
        return None
    try:
        return isinstance(instance, declared)
    except TypeError:  # a non-runtime-checkable protocol — unverifiable
        return None
