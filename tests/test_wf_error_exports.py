"""THE WF ERROR-EXPORT CONVENTION PIN: every workflow error class the
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
