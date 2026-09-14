"""Test-only :class:`JobContext` extension that adds ``deps`` for DI fixtures.

The production :class:`JobContext` lives in :mod:`taskq.context` and is
the contract every actor sees. This subclass adds a ``deps`` mapping
that the ``actor_runner`` fixture and a small number of compatibility
tests use to forward injected dependencies before the full DI scope
hierarchy ships. Tests that don't need ``deps`` should import the
production class directly.

Why this exists rather than ``deps`` on production: dependency
resolution in production goes through :func:`solve_dependencies`, which
operates on the actor's typed parameter list — not a mapping. ``deps``
on the test fixture gives actor test bodies a simple
``(payload, ctx, **deps)`` signature for injecting ad-hoc stub
collaborators until production DI is wired into actor handlers.
"""

import asyncio
import threading
from dataclasses import dataclass, field
from uuid import UUID

import structlog
from opentelemetry.trace import Span
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.client._enqueuer import SubJobEnqueuer

__all__ = ["JobContext"]


@dataclass(frozen=True, slots=True)
class JobContext[P: BaseModel]:
    """Test-scoped context with ``deps`` for fixture-injected dependencies.

    Field shape mirrors :class:`taskq.context.JobContext` (the production
    class) and adds ``deps``. The bound on ``P`` matches production —
    payload is always a :class:`pydantic.BaseModel`. Tests that pass raw
    dicts as payload should validate them through a wrapper model
    (:class:`taskq.testing.in_memory._PassthroughPayload` is the
    permissive default for ``register_stub`` callers).
    """

    job_id: JobId
    actor: str
    queue: str
    attempt: int
    payload: P
    cancel_event: asyncio.Event
    worker_id: UUID
    jobs: SubJobEnqueuer
    log: structlog.stdlib.BoundLogger
    snooze_count: int = 0
    # The fixture path is uninstrumented, so `span` holds the documented
    # OTel-disabled value production actors see when no tracer is active.
    span: Span | None = None
    deps: dict[str, object] | None = field(default=None)
    abort_requested: threading.Event = field(default_factory=threading.Event)
    # Every progress() call appends one record here — the harness half of
    # the documented progress contract: the report lands observably (the
    # actor or its test inspects this list), with `seq` strictly monotone
    # per call as production guarantees. The fixture path has no Redis/
    # Postgres wiring, so nothing is published; the ProgressTooLarge size
    # cap is a WorkerSettings concern the fixture does not carry (its
    # enforcement is pinned by the dedicated progress suites).
    progress_reports: list[dict[str, object]] = field(
        default_factory=list[dict[str, object]],
    )

    @property
    def cancellation_requested(self) -> bool:
        """True when the cancel event has been set."""
        return self.cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise :class:`asyncio.CancelledError` when cancellation has
        been requested — the production contract, so actors using the
        raising style are exercisable through the harness."""
        if self.cancel_event.is_set():
            raise asyncio.CancelledError

    def should_abort(self) -> bool:
        """Synchronous cancellation check for sync actors (thread-safe)."""
        return self.abort_requested.is_set()

    async def progress(
        self,
        *,
        step: int | None = None,
        percent: float | None = None,
        detail: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """Record a progress report on the context (see the
        ``progress_reports`` field). Signature and ``seq`` monotonicity
        mirror the production :meth:`taskq.context.JobContext.progress`;
        the harness never blocks on the network because it never
        publishes."""
        self.progress_reports.append(
            {
                "seq": len(self.progress_reports) + 1,
                "step": step,
                "percent": percent,
                "detail": detail,
                "data": data,
            }
        )
