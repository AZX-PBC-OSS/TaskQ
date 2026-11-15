"""Direct-runner ownership for declared workflow dependency factories.

A direct runner family owns products; it never registers them in the worker
cache. Child runners created by the same direct drive share that family, while
independent runners and actual event loops remain isolated. Products are still
entered, validated, pinned, and closed by :class:`WorkflowDepsOwner`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows.api._deps import deps_contract_annotations, deps_satisfied
from taskq.workflows.api._deps_factory import (
    WorkflowDepsContext,
    WorkflowDepsLifecycleError,
    WorkflowDepsOwner,
    WorkflowDepsProductTypeError,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._worker_execution import (
        _RunnerCell,  # pyright: ignore[reportPrivateUsage]  # Why: direct close reuses the accepted cell's exact pin-drain ownership, never its global registry.
    )
    from taskq.workflows.api._app import CompiledWorkflow

__all__ = ["DirectWorkflowDeps"]

logger = get_logger(__name__)
_DIRECT_GENERATION = 0
_DIRECT_CLOSE_TIMEOUT_S = 30.0


@dataclass(slots=True)
class _DirectClaim:
    """The exact owners pinned by one runner task's claimed transaction."""

    owners: list[WorkflowDepsOwner] = field(default_factory=list[WorkflowDepsOwner])


class _DirectDepsFamily:
    """Runner-local owner family, with no module-global product registry."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        schema: str,
        worker_id: JobId,
        workflow: str,
    ) -> None:
        self.pool = pool
        self.schema = schema
        # A direct family is the in-process worker scope. Child worker IDs still
        # fence their own rows, but factory construction observes this family's
        # one application-level identity and declared generation zero.
        self.worker_id = worker_id
        self.workflow = workflow
        self.loop: asyncio.AbstractEventLoop | None = None
        self.cell: _RunnerCell | None = None  # pyright: ignore[reportPrivateUsage]  # Why: this family uses P6A's bounded drain cell without entering the worker-global cache.
        self.owners: dict[int, WorkflowDepsOwner] = {}
        self.claims: dict[asyncio.Task[object], _DirectClaim] = {}
        self.closing = False

    def bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
        elif self.loop is not loop:
            raise WorkflowDepsLifecycleError(
                f"direct workflow {self.workflow!r} dependency owners belong to their "
                "creating event loop; construct an independent FlowRunner on a peer loop"
            )
        return loop

    def _drain_cell(self) -> _RunnerCell:
        # Reuse P6A's bounded pin-drain/retirement cell. It is deliberately NOT
        # inserted into _runners: independent direct families can never close or
        # evict one another, including equal worker IDs on two actual loops.
        if self.cell is None:
            from taskq.workflows._worker_execution import (
                _RunnerCell,  # pyright: ignore[reportPrivateUsage]  # Why: the direct family deliberately reuses its accepted bounded drain algorithm without global cache membership.
            )

            loop = self.bind_loop()
            self.cell = _RunnerCell((self.schema, self.workflow, self.worker_id, "direct", loop))
            self.cell.timeout = _DIRECT_CLOSE_TIMEOUT_S
        return self.cell

    def _owner_for(self, compiled: CompiledWorkflow) -> WorkflowDepsOwner:
        if self.closing:
            raise WorkflowDepsLifecycleError(
                f"direct workflow {self.workflow!r} dependency family is closed"
            )
        loop = self.bind_loop()
        factory = compiled.deps_factory
        if factory is None:
            raise WorkflowDepsLifecycleError("direct dependency resolution has no factory")
        key = id(factory)
        owner = self.owners.get(key)
        if owner is None:
            # Construction is only declared here. WorkflowDepsOwner.acquire()
            # performs the actual user factory call on the first required body.
            owner = WorkflowDepsOwner(
                factory,
                context=WorkflowDepsContext(
                    pool=self.pool,
                    schema=self.schema,
                    worker_id=self.worker_id,
                    generation=_DIRECT_GENERATION,
                    loop=loop,
                    providers=None,
                ),
                expected=deps_contract_annotations(compiled),
                timeout=_DIRECT_CLOSE_TIMEOUT_S,
            )
            self.owners[key] = owner
        return owner

    @staticmethod
    def _check_current_contract(compiled: CompiledWorkflow, product: object) -> None:
        # A same-app child may add another concrete deps annotation after the
        # shared owner was resolved. Validate that declaration without entering
        # or closing the shared product a second time.
        for body_owner, declared in deps_contract_annotations(compiled):
            if deps_satisfied(product, declared) is False:
                raise WorkflowDepsProductTypeError(
                    f"deps_factory for {body_owner!r} produced "
                    f"{type(product).__qualname__!r}, which does not satisfy "
                    f"declared deps type {getattr(declared, '__qualname__', declared)!r}"
                )

    async def acquire(self, compiled: CompiledWorkflow) -> object:
        owner = self._owner_for(compiled)
        product = await owner.acquire()
        self._check_current_contract(compiled, product)
        if self.closing:
            raise WorkflowDepsLifecycleError(
                f"direct workflow {self.workflow!r} closed during dependency construction"
            )
        task = asyncio.current_task()
        if task is None:
            raise WorkflowDepsLifecycleError("direct dependency resolution requires a Task")
        typed_task = cast("asyncio.Task[object]", task)
        claim = self.claims.get(typed_task)
        if claim is None:
            claim = _DirectClaim()
            self.claims[typed_task] = claim
        if owner not in claim.owners:
            owner.pin()
            claim.owners.append(owner)
        return product

    def release_current_claim(self) -> None:
        task = asyncio.current_task()
        if task is None:
            raise WorkflowDepsLifecycleError("direct dependency release requires a Task")
        typed_task = cast("asyncio.Task[object]", task)
        claim = self.claims.pop(typed_task, None)
        if claim is None:
            return
        self._release_claim(claim)

    def release_after_body(self, body_task: asyncio.Task[object]) -> None:
        """The typed idempotent handoff (the deadline/unwind law): the
        CURRENT task's claim is transferred to *body_task*'s done
        callback when that body is still live — the pinned products stay
        entered (and their owner pin held) until the body actually
        finishes, and the callback performs the ordinary release exactly
        once. A done body (or one with no claim) releases (or no-ops)
        synchronously."""
        task = asyncio.current_task()
        if task is None:
            raise WorkflowDepsLifecycleError("direct dependency release requires a Task")
        typed_task = cast("asyncio.Task[object]", task)
        claim = self.claims.pop(typed_task, None)
        if claim is None:
            return
        if body_task.done():
            self._release_claim(claim)
            return
        body_task.add_done_callback(lambda _t: self._release_claim(claim))

    def _release_claim(self, claim: _DirectClaim) -> None:
        cell = self._drain_cell()
        for owner in claim.owners:
            owner.release()
            if owner.retired and owner.pins == 0:
                # A bounded close that lost its pin-drain wait remains retryable
                # on this exact release; a newer wait is protected by P6A's
                # owner/task identity bookkeeping.
                cell.ensure_retired_close(owner)

    async def aclose(self) -> None:
        self.bind_loop()
        if not self.owners:
            self.closing = True
            return
        cell = self._drain_cell()
        self.closing = True
        for owner in tuple(self.owners.values()):
            owner.retire()
            cell.ensure_retired_close(owner)
        # The shared cell observes actual claim pins, uses the factory/close
        # bound, logs ordinary close failures, and revisits late releases. It
        # never force-closes a resource beneath a still-unwinding claim.
        await cell.aclose()


@dataclass(slots=True)
class DirectWorkflowDeps:
    """One FlowRunner's binding to a private direct owner family."""

    family: _DirectDepsFamily
    owns_family: bool = True

    @classmethod
    def for_runner(
        cls,
        *,
        pool: asyncpg.Pool,
        schema: str,
        worker_id: JobId,
        workflow: str,
    ) -> DirectWorkflowDeps:
        return cls(_DirectDepsFamily(pool, schema=schema, worker_id=worker_id, workflow=workflow))

    def for_child(self) -> DirectWorkflowDeps:
        """Share products with a direct descendant but never close its parent."""
        return DirectWorkflowDeps(self.family, owns_family=False)

    async def acquire(self, compiled: CompiledWorkflow) -> object:
        return await self.family.acquire(compiled)

    def release_current_claim(self) -> None:
        self.family.release_current_claim()

    def release_after_body(self, body_task: asyncio.Task[object]) -> None:
        """The typed idempotent handoff (the deadline/unwind law) — the
        family's own rule, exposed on the runner's binding."""
        self.family.release_after_body(body_task)

    async def aclose(self) -> None:
        # Closing a child refuses further child use at the FlowRunner boundary;
        # only the root binding closes the family shared with its active parent.
        if self.owns_family:
            await self.family.aclose()
