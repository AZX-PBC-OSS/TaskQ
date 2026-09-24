"""Leadership state for the hand-built ``WorkerDeps`` stands-in.

Several loop tests build a ``SimpleNamespace`` listing exactly the deps
attributes the loop under test reads, deliberately avoiding a full
``WorkerDeps``. Leadership is two coupled fields plus the predicate every
leader-gated loop consults per iteration, so a stub that lists only the
event answers a question the loops no longer ask.

Binding the production predicate here rather than restating it keeps the
stubs from drifting into their own definition of what leading means.
"""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

from taskq.worker.deps import LeaderTerm, WorkerDeps

__all__ = ["stub_deps"]


def stub_deps(stub: SimpleNamespace, *, term: LeaderTerm | None = None) -> WorkerDeps:
    """Complete *stub*'s leadership surface and present it as ``WorkerDeps``.

    ``term`` defaults to ``None``, which the predicate treats as a role
    held with no expiry to narrow it - the state a stub that sets the
    event by hand is modelling.

    A stub that lists no ``is_leader`` is left alone: it stands in for a
    seam that never consults leadership, and inventing an event for it
    would make it answer a question its test is not asking.
    """
    if hasattr(stub, "is_leader"):
        stub.leader_term = term
        # Bound to the real implementations so the stub cannot answer the
        # per-iteration gate, or move the role, differently from a live
        # worker - the coupling of the event and the term is the invariant
        # these seams exist to exercise.
        leading: Any = WorkerDeps.leading
        lead: Any = WorkerDeps.lead
        stop_leading: Any = WorkerDeps.stop_leading
        stub.leading = lambda: bool(leading(stub))
        stub.lead = lambda new_term: lead(stub, new_term)
        stub.stop_leading = lambda: stop_leading(stub)
        # The election loop reads the stop signal every iteration (the
        # shutdown-ordering contract's park: leader.py's _stopping). A
        # stub driving that loop must carry it, defaulting to "no stop
        # in progress" - without it the loop dies on AttributeError
        # before its first attempt.
        if not hasattr(stub, "shutdown_start_event"):
            stub.shutdown_start_event = asyncio.Event()
    return cast(WorkerDeps, stub)
