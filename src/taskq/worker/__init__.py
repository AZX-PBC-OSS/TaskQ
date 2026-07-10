"""Worker subsystem: pool management, deps, heartbeat, and connection budgeting.

All imports are lazy via ``__getattr__`` so that importing any submodule
(e.g. ``taskq.worker.actor_config``) does not pull in ``asyncpg`` or
concrete backends.  This keeps ``import taskq.testing`` lean.

The names below are also imported under ``TYPE_CHECKING`` so static tools
(pyright, mkdocstrings) can resolve them without triggering the runtime
imports the lazy ``__getattr__`` is designed to avoid — at type-check
time nothing is actually executed, so this costs nothing at runtime.
"""

# pyright: reportUnsupportedDunderAll=false

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskq.worker.budget import ConnectionBudget, compute_connection_budget
    from taskq.worker.cancel import ActiveJobRegistry, CancelController, make_cancel_controller
    from taskq.worker.deps import WorkerDeps, open_worker_deps
    from taskq.worker.heartbeat import heartbeat_loop, isolate_self
    from taskq.worker.shutdown import ShutdownPhase, drain_local_queue_to_pending
    from taskq.worker.startup import sync_actor_config


def __getattr__(name: str) -> object:
    _lazy = {
        "ActiveJobRegistry": lambda: _imp("taskq.worker.cancel", "ActiveJobRegistry"),
        "CancelController": lambda: _imp("taskq.worker.cancel", "CancelController"),
        "ConnectionBudget": lambda: _imp("taskq.worker.budget", "ConnectionBudget"),
        "ShutdownPhase": lambda: _imp("taskq.worker.shutdown", "ShutdownPhase"),
        "WorkerDeps": lambda: _imp("taskq.worker.deps", "WorkerDeps"),
        "compute_connection_budget": lambda: _imp(
            "taskq.worker.budget", "compute_connection_budget"
        ),
        "drain_local_queue_to_pending": lambda: _imp(
            "taskq.worker.shutdown", "drain_local_queue_to_pending"
        ),
        "heartbeat_loop": lambda: _imp("taskq.worker.heartbeat", "heartbeat_loop"),
        "isolate_self": lambda: _imp("taskq.worker.heartbeat", "isolate_self"),
        "make_cancel_controller": lambda: _imp("taskq.worker.cancel", "make_cancel_controller"),
        "open_worker_deps": lambda: _imp("taskq.worker.deps", "open_worker_deps"),
        "sync_actor_config": lambda: _imp("taskq.worker.startup", "sync_actor_config"),
    }
    if name in _lazy:
        return _lazy[name]()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _imp(module_name: str, attr: str) -> object:
    import importlib

    return getattr(importlib.import_module(module_name), attr)


__all__ = [  # pyright: ignore[reportUnsupportedDunderAll]  # Why: __getattr__ lazily provides all names — avoids pulling asyncpg at import time
    "ActiveJobRegistry",
    "CancelController",
    "ConnectionBudget",
    "ShutdownPhase",
    "WorkerDeps",
    "compute_connection_budget",
    "drain_local_queue_to_pending",
    "heartbeat_loop",
    "isolate_self",
    "make_cancel_controller",
    "open_worker_deps",
    "sync_actor_config",
]
