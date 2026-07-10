"""Re-export shim for :mod:`taskq._scope`.

Preserves the ``from taskq._di.scope import Scope`` import path used by
internal modules.  The canonical definitions live in :mod:`taskq._scope`
so that ``taskq.exceptions`` can import :class:`Scope` without triggering
``taskq._di.__init__`` (which would create a circular import through
``taskq._di.scopes`` → ``taskq.context``).
"""

from taskq._scope import LifecycleDetectionWarning, Scope

__all__ = ["LifecycleDetectionWarning", "Scope"]
