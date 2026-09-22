"""Module-namespace patching: aim a fake at the module that LOOKS IT UP.

``monkeypatch.setattr(run_mod.asyncio, "sleep", fake)`` reads as if it
scoped the fake to ``run_mod``, but ``run_mod.asyncio`` IS the global
``asyncio`` module - the patch swaps ``sleep`` on the process-global
module, so every coroutine in the process (fixture machinery on the
shared module loop, a leftover task from an earlier test, pytest-asyncio
itself) that resolves ``asyncio.sleep`` during the patched window gets
the fake. When a module-scoped fixture's long-lived task hits the fake's
timing, the failure shows up in a LATER test - an ordering dependency,
not a deterministic red (the asyncio.sleep patching incident).

The fix is to patch the name where it is looked up. A module that does
``import asyncio`` and calls ``asyncio.sleep(...)`` resolves the name
``asyncio`` in its OWN module globals at call time - so rebinding THAT
binding to a proxy (everything delegates to the real module, the faked
names are shadowed) confines the fake to the code under test and leaves
the global module untouched for every other namespace.

``module_ns_proxy`` builds that stand-in; ``patch_ns`` binds it with
``monkeypatch.setattr`` in one step. Both are intentionally dumb: no
attribute is special-cased, so a faked name that stops existing on the
real module still shadows, and delegation goes through ``getattr`` at
access time so late-added attributes are visible.
"""

import types
from typing import Any

import pytest


class _ModuleNSProxy:
    """A module-like stand-in: ``overrides`` shadow, everything delegates."""

    __slots__ = ("_overrides", "_real")

    def __init__(self, real: types.ModuleType, **overrides: Any) -> None:
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_overrides", overrides)

    def __getattr__(self, name: str) -> Any:
        # Only reached for names NOT found normally (i.e. not in
        # __slots__/overrides), so the override table itself is safe to
        # touch here.
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        return getattr(object.__getattribute__(self, "_real"), name)

    def __repr__(self) -> str:
        real: types.ModuleType = object.__getattribute__(self, "_real")
        return f"<module_ns_proxy of {real.__name__!r}>"


def module_ns_proxy(real: types.ModuleType, /, **overrides: Any) -> Any:
    """A stand-in for the module object *real* whose ``overrides`` shadow
    the original attributes and every other attribute delegates to *real*.

    Pass it to ``monkeypatch.setattr(owner, "asyncio", module_ns_proxy(
    asyncio, sleep=fake))`` where ``owner`` is the module whose code
    resolves the name - NOT to the stdlib module itself. See the module
    docstring for why the through-to-the-global spelling is a leak.
    """
    return _ModuleNSProxy(real, **overrides)


def patch_ns(
    monkeypatch: pytest.MonkeyPatch,
    owner: types.ModuleType,
    name: str,
    **overrides: Any,
) -> None:
    """Rebind attribute *name* on the owning module to a proxy of its
    current value with ``overrides`` shadowed - the one-step form of
    :func:`module_ns_proxy` for the common ``owner.<stdlibmod>`` case."""
    real = getattr(owner, name)
    monkeypatch.setattr(owner, name, module_ns_proxy(real, **overrides))
