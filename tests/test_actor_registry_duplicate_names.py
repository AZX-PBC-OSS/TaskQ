"""Pins for the duplicate-actor-name refusal at registry load.

Red (proven live at the audit base): two ``@actor(name="dup")``
decorators in one registry loaded through ``taskq.cli.
_load_actor_registry`` booted with ``actor_count == 1`` and exit 0 --
the dict comprehension ``{r.name: r for r in items}`` let the LAST
duplicate silently win, so dispatch for the loser's jobs ran the
winner's code. A silent wrong-code dispatch is the worst failure mode
a registry can have, so a duplicate name is refused at load, the
message naming the collision and BOTH registration sites.

The same ActorRef object listed twice is NOT a refusal: the dispatch
behavior is byte-identical (one function, one config), so collapsing it
preserves the old shape for restart/registry-rebuild flows.
"""

import sys
from typing import Any

import pytest
import typer
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.cli import _load_actor_registry
from taskq.retry import RetryPolicy
from taskq.testing.assertions import plain_cli_output

_THIS_MODULE = "tests.test_actor_registry_duplicate_names"


class _Payload(BaseModel):
    value: int = 0


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(name: str) -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()  # pragma: no cover - never dispatched in this test

    # Distinct quals per call so the refusal message's two sites differ.
    _handler.__qualname__ = f"_handler_{name}"  # type: ignore[misc]
    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def _install(mod_attr: str, value: Any) -> str:
    """Stash *value* on this module as ``_<mod_attr>`` and return the ref."""
    setattr(sys.modules[_THIS_MODULE], f"_{mod_attr}", value)
    return f"{_THIS_MODULE}:_{mod_attr}"


def test_duplicate_names_refused_naming_both_sites(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Two distinct ActorRef objects sharing one name refuse to load."""
    first = _make_actor_ref("dup")
    second = _make_actor_ref("dup")
    assert first is not second
    ref = _install("DUP_PAIR", [first, second])
    with pytest.raises(typer.Exit) as excinfo:
        _load_actor_registry(ref)
    assert excinfo.value.exit_code == 1
    err = plain_cli_output(capsys.readouterr().err)
    assert "duplicate actor name 'dup'" in err
    # Both registration sites named (the same qualname twice here, once
    # per site), and the collision names the registry it came from.
    assert str(ref) in err
    assert err.count("_handler_dup") == 2


def test_same_object_listed_twice_still_collapses() -> None:
    """The legit-reregistration carve-out: the SAME ActorRef object twice
    is not a collision (one function, one config, nothing to mis-dispatch)
    and keeps the old collapse behavior."""
    the_ref = _make_actor_ref("dup")
    ref = _install("SAME_TWICE", [the_ref, the_ref])
    registry = _load_actor_registry(ref)
    assert dict(registry) == {"dup": the_ref}


def test_mapping_registry_with_unique_names_unaffected() -> None:
    """The Mapping path never had a duplicate hazard (keys are unique by
    construction) and must be untouched by the refusal."""
    a = _make_actor_ref("a")
    b = _make_actor_ref("b")
    ref = _install("MAPPED", {"a": a, "b": b})
    registry = _load_actor_registry(ref)
    assert dict(registry) == {"a": a, "b": b}


def test_distinct_names_still_load() -> None:
    """The refusal is name-collision-specific: distinct names load as before."""
    a = _make_actor_ref("a")
    b = _make_actor_ref("b")
    ref = _install("LISTED", [a, b])
    registry = _load_actor_registry(ref)
    assert dict(registry) == {"a": a, "b": b}
