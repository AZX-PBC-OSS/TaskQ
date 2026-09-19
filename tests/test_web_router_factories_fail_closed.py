"""Router-factory fail-closed sweep: a NEW public router factory fails here
until its auth gate is proven.

The class this file guards: a ``create_router`` factory under ``taskq.web``
that serves its routes without authentication and without raising.
``taskq.web.admin.create_router`` has always failed closed - ``RuntimeError``
at factory time when ``auth_dependency`` is ``None`` outside a dev
environment - while ``taskq.web.progress.create_router`` shipped without the
gate and exposed per-job state and an anonymously-bounded SSE surface in
exactly the deployments that needed it closed. Both factories carry the gate
now, with their own per-factory tests. Those tests guard their factory. This
file guards the *surface*: it walks every module under ``taskq.web``, so the
third factory fails the registry check on arrival, and the behavioural half
proves the gate rather than trusting the registration.

Precedent: ``tests/test_sweepaudit_bounded_writes.py`` - per-site pins guard
each known site, a walked registry catches the next one.

What to do when the registry check fails on a factory you added: nothing in
this file needs editing unless the factory is deliberate. If it raises
without auth outside dev (the required shape), add its qualified name to
``_KNOWN_FACTORIES`` and the behavioural test covers it from then on. If you
believe a new factory should serve anonymously in production, that is a
security decision - take it to review before registering an exemption, and
expect the review to ask why the sibling pattern
(``TASKQ_*_REQUIRE_AUTH=false`` opt-out, loud warning when suppressed) does
not fit.
"""

import inspect
import pkgutil
from collections.abc import Callable
from importlib import import_module
from types import ModuleType
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

import taskq.web

#: Router factories known to carry the fail-closed gate, keyed by
#: ``module.qualname`` at the definition site (re-exports are deduplicated by
#: ``__module__``). The value names the factory's own gate test file, where
#: the environment-scoped and opt-out halves of its contract are pinned.
pytestmark = [pytest.mark.fastapi]
_KNOWN_FACTORIES: dict[str, str] = {
    "taskq.web.progress.create_router": "tests/web_progress/test_auth_gate.py",
    "taskq.web.admin._factory.create_router": "tests/web_admin/test_factory.py",
}

#: Stub arguments for required factory parameters, by parameter name. A
#: factory whose required parameter is not registered here fails the
#: behavioural test with an instruction - the missing stub is the guard
#: noticing a factory shape it cannot drive, not a pass.
_STUB_ARGS: dict[str, Any] = {}


class _StubPool:
    """Duck-typed stand-in for asyncpg.Pool - the factories only store it."""


_STUB_ARGS["pg_pool"] = _StubPool()
_STUB_ARGS["redis_client"] = None


def _discover_router_factories() -> tuple[dict[str, Callable[..., Any]], list[str]]:
    """Walk every module under ``taskq.web`` and collect functions named
    ``create_router`` at their definition site. Returns the factories keyed
    by qualified name, plus the names of modules that failed to import - a
    skipped module is a blind spot in the sweep, so the caller asserts the
    list is empty rather than letting an unimportable module hide a factory.
    """
    found: dict[str, Callable[..., Any]] = {}
    skipped: list[str] = []
    for module_info in pkgutil.walk_packages(taskq.web.__path__, prefix="taskq.web."):
        try:
            module: ModuleType = import_module(module_info.name)
        except ImportError:
            skipped.append(module_info.name)
            continue
        for name, value in inspect.getmembers(module, inspect.isfunction):
            if name == "create_router" and value.__module__ == module.__name__:
                found[f"{value.__module__}.{value.__qualname__}"] = value
    return found, skipped


def _invoke_without_auth(factory: Callable[..., Any], qualname: str) -> object:
    """Call the factory with ``auth_dependency=None`` and stubs for every
    required parameter. An unregistered required parameter fails loudly:
    the sweep must be able to drive every factory it watches."""
    kwargs: dict[str, Any] = {}
    for param_name, param in inspect.signature(factory).parameters.items():
        if param_name == "auth_dependency":
            kwargs[param_name] = None
        elif param.default is inspect.Parameter.empty:
            assert param_name in _STUB_ARGS, (
                f"{qualname} requires parameter {param_name!r}, which has no stub "
                "registered in _STUB_ARGS. Add a stub so the fail-closed sweep can "
                "drive this factory."
            )
            kwargs[param_name] = _STUB_ARGS[param_name]
    return factory(**kwargs)


def test_router_factory_registry_matches_the_walked_surface() -> None:
    """The registry half: the set of discovered factories equals the
    registered set, both ways - a new factory fails on arrival, a removed or
    renamed factory fails on staleness, and an unimportable module fails
    rather than silently narrowing the sweep."""
    discovered, skipped = _discover_router_factories()
    assert not skipped, (
        f"modules under taskq.web failed to import and were not swept: {sorted(skipped)}. "
        "A router factory hidden behind an import error is invisible to this guard."
    )
    assert set(discovered) == set(_KNOWN_FACTORIES), (
        f"router factory surface drifted: discovered {sorted(discovered)}, "
        f"registered {sorted(_KNOWN_FACTORIES)}. A new create_router under taskq.web "
        "must fail closed without auth before it is registered here - see this "
        "file's docstring."
    )


def test_every_router_factory_fails_closed_without_auth_outside_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The behavioural half: every registered factory raises RuntimeError
    naming auth when called with auth_dependency=None under a non-dev
    environment - the gate itself, not the registration."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    discovered, _ = _discover_router_factories()
    for qualname in _KNOWN_FACTORIES:
        factory = discovered[qualname]
        with pytest.raises(RuntimeError, match="auth"):
            _invoke_without_auth(factory, qualname)
