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
    """``SpawnResultModelError`` (the spawn's ``result=`` refusal, #749)
    is a top-level ``taskq.workflows`` export, IDENTICAL to its
    defining-module class and a member of ``__all__``."""
    import taskq.workflows as wf
    import taskq.workflows._childflow as childflow
    from taskq.workflows import SpawnResultModelError

    assert "SpawnResultModelError" in wf.__all__
    assert SpawnResultModelError is childflow.SpawnResultModelError


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
