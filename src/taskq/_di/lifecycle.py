"""Lifecycle shape detection for DI providers."""

import inspect

import structlog

from taskq._di.types import Factory, ProviderLifecycle

logger = structlog.get_logger("taskq._di.lifecycle")


def detect_lifecycle(cls: type) -> ProviderLifecycle:
    """Inspect a class and return its lifecycle shape.

    Priority order:
      1. hasattr(cls, '__aenter__') and hasattr(cls, '__aexit__') → AsyncContextManager
      2. hasattr(cls, 'aclose')                                   → AsyncCloseable
      3. hasattr(cls, 'close')                                    → SyncCloseable
      4. otherwise                                                → Plain

    Pure — no instantiation, no warnings, no logs, no side effects.
    The caller (register_class) owns all WARNING emissions.
    """
    if hasattr(cls, "__aenter__") and hasattr(cls, "__aexit__"):
        return ProviderLifecycle.AsyncContextManager
    if hasattr(cls, "aclose"):
        return ProviderLifecycle.AsyncCloseable
    if hasattr(cls, "close"):
        return ProviderLifecycle.SyncCloseable
    return ProviderLifecycle.Plain


def detect_factory_lifecycle(factory: Factory[object]) -> ProviderLifecycle:
    """Inspect a factory callable and return its lifecycle shape.

    Priority order:
      1. inspect.isasyncgenfunction(factory)  → AsyncGenerator
      2. inspect.isgeneratorfunction(factory) → SyncGenerator
      3. otherwise                            → PlainFactory

    Pure — no warnings, no logs, no side effects, no invocation.
    The caller (register_factory) owns all WARNING emissions .

    factory: Factory[object] — detection inspects code flags, not the
        produced type T; the caller (register_factory[T]) already
        enforces Factory[T].
    """
    if inspect.isasyncgenfunction(factory):
        return ProviderLifecycle.AsyncGenerator
    if inspect.isgeneratorfunction(factory):
        return ProviderLifecycle.SyncGenerator
    return ProviderLifecycle.PlainFactory
