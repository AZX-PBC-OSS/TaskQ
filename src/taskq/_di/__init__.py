"""Internal DI package — resolution engine and type vocabulary."""

from taskq._di.lifecycle import detect_factory_lifecycle, detect_lifecycle
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.scopes import (
    LoopScope,
    ProcessScope,
    ResolvedActorScope,
    ScopeContainer,
    ThreadScope,
    build_actor_scope,
)
from taskq._di.types import (
    Factory,
    FactoryShape,
    ProviderEntry,
    ProviderLifecycle,
)
from taskq._di.types import (
    ProviderRegistry as ProviderRegistryProtocol,
)
from taskq._di.types import (
    ScopeContainer as ScopeContainerProtocol,
)

__all__ = [
    "Factory",
    "FactoryShape",
    "LifecycleDetectionWarning",
    "LoopScope",
    "ProcessScope",
    "ProviderEntry",
    "ProviderLifecycle",
    "ProviderRegistry",
    "ProviderRegistryProtocol",
    "ResolvedActorScope",
    "Scope",
    "ScopeContainer",
    "ScopeContainerProtocol",
    "ThreadScope",
    "build_actor_scope",
    "detect_factory_lifecycle",
    "detect_lifecycle",
]
