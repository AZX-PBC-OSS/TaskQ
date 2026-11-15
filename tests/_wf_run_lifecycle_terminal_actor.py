"""Typed terminal-settlement actor fixture for the run-lifecycle controls.

The fixture is imported only after the controls have asserted that the public
``WorkflowTerminalEvent`` export exists.  It is ordinary statically typed code:
the actor handler names the real event model, so the decorator resolves the
payload type without annotation mutation or caller-side casts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from taskq.actor import ActorRef, actor
from taskq.workflows import (
    WorkflowTerminalEvent,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the public event type is the future API the controls pin.
)

TerminalSettlement = Callable[[WorkflowTerminalEvent], Awaitable[None]]


def make_terminal_actor(  # pyright: ignore[reportUnknownParameterType]  # Why: the future event type is unknown until its public export lands.
    name: str,
    queue: str,
    body: TerminalSettlement,  # pyright: ignore[reportUnknownParameterType]  # Why: future event type unknown until export.
) -> ActorRef[WorkflowTerminalEvent, None]:
    """Build one ordinary typed settlement actor for the declared workflow."""

    async def handler(event: WorkflowTerminalEvent) -> None:  # pyright: ignore[reportUnknownParameterType]  # Why: same future export.
        await body(event)

    return actor(name=name, queue=queue)(handler)
