"""DI scope enum and lifecycle warning — zero-dependency leaf module.

Extracted from ``taskq._di.scope`` so that ``taskq.exceptions`` can import
:class:`Scope` without triggering ``taskq._di.__init__`` (which loads
``taskq._di.scopes`` → ``taskq.context`` → circular import).

This module MUST NOT import from any other ``taskq`` submodule.
"""

from enum import IntEnum

__all__ = ["LifecycleDetectionWarning", "Scope"]


class Scope(IntEnum):
    """DI scope lifetime. Higher value = narrower scope.

    PROCESS:   worker process startup -> exit (singletons).
    THREAD:    worker thread spawn -> thread close (placeholder for multi-thread worker).
    LOOP:      worker loop start -> loop close (pools, long-lived clients).
    TRANSIENT: fresh per injection point - no cache.
    """

    PROCESS = 0
    THREAD = 1
    LOOP = 2
    TRANSIENT = 3


class LifecycleDetectionWarning(UserWarning):
    """Emitted for DI usage hazards detected during DI operations.

     Specific timing varies per emission site and is documented at
     each emission. Fires for redundant call-site Scope overrides
    , sync close detection on registered
     classes, and other drift-prone API patterns. Subclass
     of UserWarning so pytest captures it under default filters via
     pytest.warns(LifecycleDetectionWarning).
    """
