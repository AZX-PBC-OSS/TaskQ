"""Coverage for ``taskq.worker.__init__`` lazy ``__getattr__`` re-exports.

The worker package re-exports symbols from submodules lazily so that
importing ``taskq.worker`` does not pull in ``asyncpg`` or concrete
backends.  These tests verify every name in ``__all__`` resolves to the
same object as a direct submodule import, and that unknown names raise
``AttributeError``.
"""

import importlib

import pytest

import taskq.worker

# (public name, source module, source attr)
_EXPORTS: list[tuple[str, str, str]] = [
    ("ActiveJobRegistry", "taskq.worker.cancel", "ActiveJobRegistry"),
    ("CancelController", "taskq.worker.cancel", "CancelController"),
    ("ConnectionBudget", "taskq.worker.budget", "ConnectionBudget"),
    ("ShutdownPhase", "taskq.worker.shutdown", "ShutdownPhase"),
    ("WorkerDeps", "taskq.worker.deps", "WorkerDeps"),
    ("compute_connection_budget", "taskq.worker.budget", "compute_connection_budget"),
    ("drain_local_queue_to_pending", "taskq.worker.shutdown", "drain_local_queue_to_pending"),
    ("heartbeat_loop", "taskq.worker.heartbeat", "heartbeat_loop"),
    ("isolate_self", "taskq.worker.heartbeat", "isolate_self"),
    ("make_cancel_controller", "taskq.worker.cancel", "make_cancel_controller"),
    ("open_worker_deps", "taskq.worker.deps", "open_worker_deps"),
    ("sync_actor_config", "taskq.worker.startup", "sync_actor_config"),
]


def test_all_exports_listed() -> None:
    """Every (name, module, attr) tuple in ``_EXPORTS`` is in ``__all__``."""
    for name, _mod, _attr in _EXPORTS:
        assert name in taskq.worker.__all__, f"{name!r} missing from taskq.worker.__all__"


@pytest.mark.parametrize("name,module_name,attr", _EXPORTS)
def test_each_export_resolves(name: str, module_name: str, attr: str) -> None:
    """Each lazy export resolves to the same object as a direct import."""
    resolved = getattr(taskq.worker, name)
    direct = getattr(importlib.import_module(module_name), attr)
    assert resolved is direct, f"taskq.worker.{name} is not {module_name}.{attr}"


def test_unknown_attribute_raises_attribute_error() -> None:
    """An attribute not in the lazy map raises AttributeError with the module name."""
    with pytest.raises(AttributeError, match=r"has no attribute"):
        _ = taskq.worker.does_not_exist  # type: ignore[attr-defined]


def test_exports_are_callable_or_class() -> None:
    """Each resolved export is a callable (function or class)."""
    for name, _mod, _attr in _EXPORTS:
        resolved = getattr(taskq.worker, name)
        assert callable(resolved), f"taskq.worker.{name} is not callable"


def test_imp_helper_returns_attribute() -> None:
    """The private ``_imp`` helper imports a module and returns an attr."""
    from taskq.worker import _imp

    result = _imp("taskq.worker.cancel", "ActiveJobRegistry")
    from taskq.worker.cancel import ActiveJobRegistry

    assert result is ActiveJobRegistry
