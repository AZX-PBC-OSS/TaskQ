"""The flow RUNNER's error type (the split of ``api/_runner``): the loud
refusal a caller can catch — the graph is not registered, or the flow
row is missing, or the machinery's own bound was exhausted. A caller
bug, refused loudly; never a silent wedge.
"""

from __future__ import annotations

__all__ = ["WorkflowRunError"]


class WorkflowRunError(RuntimeError):
    """The run cannot proceed — the graph is not registered, or the flow
    row is missing (a caller bug, refused loudly)."""
