"""Public DI declaration surface."""

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.types import ProviderLifecycle

__all__ = ["LifecycleDetectionWarning", "ProviderLifecycle", "ProviderRegistry", "Scope"]
