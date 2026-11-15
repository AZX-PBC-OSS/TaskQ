"""Workflow cancellation observation on the attempt's existing carrier.

The durable cancelled root is the only run-cancellation authority.  NOTIFY and
heartbeat only cause a fresh row read; neither can author body-visible state.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol

from taskq.backend._protocol import JobId
from taskq.context import CancelOrigin


class CancellationFace(Protocol):
    """The vanilla per-attempt state shared with a workflow StepContext."""

    @property
    def cancel_event(self) -> asyncio.Event: ...

    @property
    def cancel_origin(self) -> CancelOrigin: ...

    def _set_cancel_origin(self, origin: CancelOrigin) -> None: ...


@dataclass(slots=True)
class WorkflowCancelDetail:
    """Mutable, attempt-owned details installed before the event is set."""

    reason: str | None = None
    principal: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowJobRegistration:
    """The workflow facts a worker registry entry needs for observation."""

    flow_id: JobId
    detail: WorkflowCancelDetail = field(default_factory=WorkflowCancelDetail)


class CancellationEntry(Protocol):
    """The narrow active-registry surface used by the durable observer."""

    @property
    def ctx(self) -> CancellationFace: ...

    @property
    def workflow(self) -> WorkflowJobRegistration | None: ...

    cancel_origin: CancelOrigin


def publish_operator_cancellation(
    entry: CancellationEntry,
    *,
    reason: str | None,
    principal: str | None,
) -> bool:
    """Publish only after the observer proved the cancelled root in Postgres.

    There is deliberately no ``await`` in this critical section and no vanilla
    cancel-phase advancement or task cancellation.  OPERATOR retains precedence
    over an earlier SHUTDOWN observation in both registry and context.
    """
    registration = entry.workflow
    if registration is None:
        return False
    registration.detail.reason = reason
    registration.detail.principal = principal
    entry.cancel_origin = CancelOrigin.OPERATOR
    entry.ctx._set_cancel_origin(CancelOrigin.OPERATOR)  # pyright: ignore[reportPrivateUsage]  # the vanilla origin's designated internal write seam
    entry.ctx.cancel_event.set()
    return True
