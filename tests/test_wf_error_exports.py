"""The wf error-export convention pin: every workflow error class the
docs teach is importable from the package root (``taskq.workflows``) —
the convention the sibling classes ride (``ChildFlowLostError``,
``ChildWorkflowUnknownError``, ``SpawnDepthExceededError``,
``LoopWaitShapeError``, ``WorkflowBuildError``, ``WorkflowRunError``,
``RouterNotTotal`` are all root exports). The fan-in bound's class rode
only ``taskq.workflows.definitions`` while the guide documents the
top-level face; this pin holds the root export so the package's surface
and the docs' claim cannot drift apart again.
"""

from __future__ import annotations


def test_fan_in_bound_error_is_a_top_level_workflow_export() -> None:
    """``FanInBoundExceededError`` is a top-level ``taskq.workflows``
    export, per the sibling error classes' convention the guide
    documents. Three assertions: importable from the root, IDENTICAL to
    the definitions class (a re-export, never a shadow), and a member of
    ``__all__`` (the declared public surface)."""
    import taskq.workflows as wf
    import taskq.workflows.definitions as definitions
    from taskq.workflows import FanInBoundExceededError

    assert "FanInBoundExceededError" in wf.__all__
    assert FanInBoundExceededError is definitions.FanInBoundExceededError


def test_emit_reservation_exceeded_error_is_a_top_level_workflow_export() -> None:
    """``EmitReservationExceededError`` is a top-level ``taskq.workflows``
    export by the same convention: importable from the root, IDENTICAL to
    its defining-module class (a re-export, never a shadow), a member of
    ``__all__``, and derived EXACTLY from ``Exception`` — never from
    ``EmitBackpressure`` or ``PageDivergedError``."""
    import taskq.workflows as wf
    import taskq.workflows._emit as emit_module
    from taskq.workflows import EmitReservationExceededError

    assert "EmitReservationExceededError" in wf.__all__
    assert EmitReservationExceededError is emit_module.EmitReservationExceededError
    assert EmitReservationExceededError.__bases__ == (Exception,)


def test_spawn_result_model_error_is_a_top_level_workflow_export() -> None:
    """``SpawnResultModelError`` (the spawn's ``result=`` refusal) is a
    top-level ``taskq.workflows`` export, IDENTICAL to its defining-module
    class and a member of ``__all__``."""
    import taskq.workflows as wf
    import taskq.workflows._childflow as childflow
    from taskq.workflows import SpawnResultModelError

    assert "SpawnResultModelError" in wf.__all__
    assert SpawnResultModelError is childflow.SpawnResultModelError


def test_child_result_contract_error_is_a_top_level_workflow_export() -> None:
    """``ChildResultContractError`` (the join's decode refusal) is a
    top-level ``taskq.workflows`` export by the same convention: a
    ``ValueError`` (the shape pydantic's ``ValidationError`` had), the
    defining-module class itself, and a member of ``__all__``."""
    import taskq.workflows as wf
    import taskq.workflows._childflow as childflow
    from taskq.workflows import ChildResultContractError

    assert "ChildResultContractError" in wf.__all__
    assert ChildResultContractError is childflow.ChildResultContractError
    assert issubclass(ChildResultContractError, ValueError)


def test_every_childflow_symbol_the_root_reexports_is_in_the_module_all() -> None:
    """``_childflow.__all__`` is the module's own declaration of the surface
    it offers, and ``taskq.workflows`` re-exports from it. The two lists must
    agree: every name the package root re-exports out of ``_childflow`` must
    appear in ``_childflow.__all__``. ``RUF022`` enforces sortedness only, so
    a MISSING member is invisible to lint."""
    _childflow_module = "taskq.workflows._childflow"
    import taskq.workflows as wf
    import taskq.workflows._childflow as childflow

    reexported = {
        name
        for name in wf.__all__
        if getattr(getattr(wf, name, None), "__module__", None) == _childflow_module
    }
    # The witness: the walk is real and non-vacuous at this head.
    assert {"ChildOk", "ChildWorkflowUnknownError", "SpawnDepthExceededError"} <= reexported, (
        f"the re-export walk found nothing — the fixture head rotted: {sorted(reexported)}"
    )
    missing = sorted(reexported - set(childflow.__all__))
    assert not missing, (
        "taskq.workflows re-exports these out of taskq.workflows._childflow but the "
        f"defining module's __all__ omits them: {missing} — the module's declared "
        "surface is shorter than the surface it actually supplies"
    )


def test_map_index_schema_error_is_a_top_level_workflow_export() -> None:
    """``MapIndexSchemaError`` is a top-level ``taskq.workflows`` export by the
    same convention: the docs' deployment-error guidance names it, so the
    package surface must carry it. Importable from the root, IDENTICAL to its
    defining-module class (a re-export, never a shadow), and in ``__all__``."""
    import taskq.workflows as wf
    import taskq.workflows._types as types_module
    from taskq.workflows import MapIndexSchemaError

    assert "MapIndexSchemaError" in wf.__all__
    assert MapIndexSchemaError is types_module.MapIndexSchemaError


def test_duplicate_substep_error_is_a_top_level_workflow_export() -> None:
    """``DuplicateSubstepError`` is a top-level ``taskq.workflows`` export by
    the same convention: both guides teach the class to the author who must
    catch it, so the package surface must carry it. Importable from the root,
    IDENTICAL to its defining-module class (a re-export, never a shadow), and
    in ``__all__``."""
    import taskq.workflows as wf
    import taskq.workflows.api._runner_errors as runner_errors
    from taskq.workflows import DuplicateSubstepError

    assert "DuplicateSubstepError" in wf.__all__
    assert DuplicateSubstepError is runner_errors.DuplicateSubstepError


def test_spawn_key_reserved_error_is_a_top_level_workflow_export() -> None:
    """``SpawnKeyReservedError`` is a top-level ``taskq.workflows`` export by
    the sibling error classes' convention: the guide documents the bare-
    decimal refusal, so the package surface carries the class. Importable
    from the root, IDENTICAL to its defining-module class, in ``__all__``,
    and a ``ValueError`` subclass (callers that guarded the older plain
    refusal keep working)."""
    import taskq.workflows as wf
    import taskq.workflows._childflow as childflow
    from taskq.workflows import SpawnKeyReservedError

    assert "SpawnKeyReservedError" in wf.__all__
    assert SpawnKeyReservedError is childflow.SpawnKeyReservedError
    assert issubclass(SpawnKeyReservedError, ValueError)
