"""Tests for redundant-override dual-signal emitted at validate() time.

Scenarios:
  - Redundant override at validate() emits dual signal
  - Non-redundant override does NOT emit
  - Override that differs from registered default does NOT emit
  - Multiple overrides on one actor emit per parameter
  - Two actors with the same redundant override emit per actor
  - Idempotency: validate() called twice emits only once
  - Solver no longer emits the warning at dispatch
  - _redundant_override_seen is gone from the solver module
"""

import warnings
from pathlib import Path
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._di import solver as _solver
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import LifecycleDetectionWarning, Scope
from taskq._di.solver import solve_dependencies
from taskq._di.types import ProviderEntry
from taskq.actor import ActorRef
from taskq.backend.clock import Clock, SystemClock
from taskq.retry import RetryPolicy


class _Payload(BaseModel):
    x: int


class _Result(BaseModel):
    y: int


class _Settings:
    pass


class _SvcA:
    pass


class _SvcB:
    pass


class _SvcC:
    pass


def _make_actor_ref(
    name: str,
    fn: Any,
) -> ActorRef[Any, Any]:
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
    )


# ── Redundant override at validate() emits dual signal ────────────


def test_redundant_override_at_validate_emits_dual_signal() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(payload: _Payload, settings: Annotated[_Settings, Scope.PROCESS]) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with pytest.warns(LifecycleDetectionWarning) as record:
        registry.validate(actors=[ref])

    assert len(record) == 1
    msg = str(record[0].message)
    assert "my_actor" in msg
    assert "settings" in msg
    assert "PROCESS" in msg


def test_redundant_override_warning_points_at_validate_caller() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(payload: _Payload, settings: Annotated[_Settings, Scope.PROCESS]) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        registry.validate(actors=[ref])

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 1
    warning = lifecycle_warnings[0]
    assert Path(warning.filename).name != "registry.py", (
        "stacklevel must skip validate(); warning should point at validate()'s caller"
    )
    assert Path(warning.filename).name != "_validate.py", (
        "stacklevel must skip _emit_redundant_override_warnings(); warning should point at validate()'s caller"
    )


# ── Non-redundant override does NOT emit ──────────────────────────


def test_non_redundant_override_no_emit() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(payload: _Payload, settings: _Settings) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        registry.validate(actors=[ref])

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 0


# ── Override that differs from registered default does NOT emit ────


def test_different_override_no_emit() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(
        payload: _Payload,
        settings: Annotated[_Settings, Scope.LOOP],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        registry.validate(actors=[ref])

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 0


# ── Multiple overrides on one actor ────────────────────────────────


def test_multiple_redundant_overrides_on_one_actor() -> None:
    registry = ProviderRegistry()
    registry.register_value(_SvcA, Scope.LOOP, _SvcA())
    registry.register_value(_SvcB, Scope.LOOP, _SvcB())
    registry.register_value(_SvcC, Scope.LOOP, _SvcC())

    async def actor(
        payload: _Payload,
        a: Annotated[_SvcA, Scope.LOOP],
        b: Annotated[_SvcB, Scope.LOOP],
        c: Annotated[_SvcC, Scope.LOOP],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("multi_actor", actor)

    with pytest.warns(LifecycleDetectionWarning) as record:
        registry.validate(actors=[ref])

    assert len(record) == 3


# ── Two actors with the same redundant override ────────────────────


def test_two_actors_same_redundant_override() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor_alpha(
        payload: _Payload,
        settings: Annotated[_Settings, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    async def actor_beta(
        payload: _Payload,
        settings: Annotated[_Settings, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    ref_alpha = _make_actor_ref("actor_alpha", actor_alpha)
    ref_beta = _make_actor_ref("actor_beta", actor_beta)

    with pytest.warns(LifecycleDetectionWarning) as record:
        registry.validate(actors=[ref_alpha, ref_beta])

    assert len(record) == 2


# ── Idempotency: validate() called twice emits only once ──────────


def test_idempotent_validate_emits_once() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(
        payload: _Payload,
        settings: Annotated[_Settings, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with pytest.warns(LifecycleDetectionWarning):
        registry.validate(actors=[ref])

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        registry.validate(actors=[ref])

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 0


# ── Solver no longer emits the warning at dispatch ────────────────


async def test_solver_no_longer_emits_at_dispatch() -> None:
    registry = ProviderRegistry()
    registry.register_value(_Settings, Scope.PROCESS, _Settings())

    async def actor(
        payload: _Payload,
        settings: Annotated[_Settings, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)
    registry.validate(actors=[ref])

    class _StubContainer:
        @property
        def last_cache_hit(self) -> bool:
            return False

        async def get_or_create(self, type_: type, entry: ProviderEntry[object]) -> object:
            return entry.impl

    containers = {scope: _StubContainer() for scope in Scope}

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        await solve_dependencies(
            func=actor,
            registry=registry,
            scope_containers=containers,
            passthrough_kwargs={"payload": _Payload(x=1)},
        )

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 0


# ── _redundant_override_seen is gone from solver ───────────────────


def test_redundant_override_seen_memo_removed() -> None:
    assert not hasattr(_solver, "_redundant_override_seen")


# ── redundant Annotated[Clock, Scope.PROCESS] emits both signals ──


def test_redundant_clock_override_emits_both_signals() -> None:
    """Redundant Annotated[Clock, Scope.PROCESS] emits both signals."""
    registered = SystemClock()
    registry = ProviderRegistry()
    registry.register_value(Clock, Scope.PROCESS, registered)

    async def actor(
        payload: _Payload,
        clock: Annotated[Clock, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        registry.validate(actors=[ref])

    lifecycle_warnings = [x for x in w if issubclass(x.category, LifecycleDetectionWarning)]
    assert len(lifecycle_warnings) == 1
    msg = str(lifecycle_warnings[0].message)
    assert "my_actor" in msg
    assert "clock" in msg
    assert "PROCESS" in msg


async def test_redundant_clock_override_actor_still_resolves() -> None:
    """Redundant Annotated[Clock, Scope.PROCESS] actor still resolves correctly."""
    registered = SystemClock()
    registry = ProviderRegistry()
    registry.register_value(Clock, Scope.PROCESS, registered)

    async def actor(
        payload: _Payload,
        clock: Annotated[Clock, Scope.PROCESS],
    ) -> _Result:
        return _Result(y=1)

    ref = _make_actor_ref("my_actor", actor)
    registry.validate(actors=[ref])

    class _StubContainer:
        @property
        def last_cache_hit(self) -> bool:
            return False

        async def get_or_create(self, type_: type, entry: ProviderEntry[object]) -> object:
            return entry.impl

    containers = {scope: _StubContainer() for scope in Scope}

    result = await solve_dependencies(
        func=actor,
        registry=registry,
        scope_containers=containers,
        passthrough_kwargs={"payload": _Payload(x=1)},
    )

    assert result["clock"] is registered
