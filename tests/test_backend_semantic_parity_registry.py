"""Backend semantic-parity registry: a NEW dispatch/cancel seam fails here
until its in-memory-vs-Postgres *behaviour* is classified.

The class this file guards: a seam where ``InMemoryBackend`` and
``PostgresBackend`` return DIFFERENT RESULTS for the same inputs. Not
different objects — different answers. Three instances were found this
way, and all three passed every existing guard until each was discovered
by hand. All three are fixed and behaviourally pinned in
``tests/test_in_memory_dispatch_parity.py``; this registry exists so the
next one fails on arrival instead of waiting to be found the same way:

1. An empty ``queues`` list meant "match ALL" in memory
   (``testing/_dispatch.py`` filtered with ``not queues or row.queue in
   queues``) and "match NOTHING" in PG (``backend/_dispatch_sql.py``: an
   empty ``unnest`` in a ``CROSS JOIN LATERAL`` annihilates the candidate
   set).
2. A NULL ``fairness_key`` got a SINGLETON partition per job in memory
   (``f"__null__{r.id}"``) and ONE SHARED partition in PG
   (``PARTITION BY COALESCE(j2.fairness_key, '__null__')``), so the
   unkeyed cohort consumed a whole bounded batch in the mirror while PG
   admits one job per cohort. ``fairness_key`` is None by default.
3. ``cancel_where`` returned ids sorted by UUID in PG
   (``array_agg(id ORDER BY id)``) and by the default priority-first
   ``_list_jobs`` ordering in memory.

Why the existing guards could not catch any of them:
``test_in_memory_read_isolation.py`` and ``test_in_memory_seam_registry.py``
pin ALIASING (does a seam hand out stored objects?) and SURFACE
COMPLETENESS (is every public member classified?). Both reported green at
the same commit where the two backends demonstrably selected different
rows. The constitution requires the mirror to be "observably equivalent
... at every seam"; those files implement a strict subset of that and
read as though they implement all of it. This file guards the remaining
half: SEMANTICS.

Precedent: ``tests/test_sweepaudit_bounded_writes.py`` and
``tests/test_web_router_factories_fail_closed.py`` — per-site behavioural
pins guard each known site, a registry over the walked surface catches the
next one.

What to do when the completeness check fails on a seam you added:

* If the seam SELECTS or ORDERS rows (a dispatch, a filtered read, a bulk
  mutation returning ids), add a behavioural parity test driving BOTH
  backends through identical inputs — the shape lives in
  ``tests/test_in_memory_dispatch_parity.py`` — and register the pin here.
* If the seam cannot diverge (it returns a scalar, or it has no in-memory
  counterpart at all), register it in ``_NO_SEMANTIC_SURFACE`` with the
  reason. That sentence is the review.

One constraint this file encodes, learned by building the parity tests:
parity at the dispatch seam is definable as SELECTION parity, not ORDERING
parity. ``DISPATCH_*_SQL`` ends in ``UPDATE ... RETURNING j.*``, and
``UPDATE ... RETURNING`` carries no row-order guarantee — the ``ORDER BY``
inside the CTE governs which rows the ``LIMIT`` admits, not the order they
come back. A parity assertion on returned sequence is flaky against PG
itself. Compare claimed SETS under a discriminating bound.
"""

import inspect
from collections.abc import Callable
from typing import Any

from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

#: Seams whose result depends on SELECTION or ORDERING of rows, and which
#: therefore owe the two backends the same answer. Maps the method name to
#: the file holding its behavioural parity pin.
_SEMANTIC_SEAMS: dict[str, str] = {
    "dispatch_batch": "tests/test_in_memory_dispatch_parity.py",
    "cancel_where": "tests/test_in_memory_dispatch_parity.py",
}

#: Seams that SELECT or ORDER rows and are NOT yet pinned by a parity test.
#: Every entry is a known gap, not an exemption: the two backends could
#: answer differently here and nothing would notice.
#:
#: Moving an entry out of here means writing its parity test. Do not move
#: one into _NO_SEMANTIC_SURFACE without establishing that the seam makes no
#: selection or ordering decision — that claim is what the registry exists
#: to force someone to make explicitly.
_SEMANTIC_SEAMS_UNPINNED: dict[str, str] = {
    "list_jobs": "filter predicates, cursor ordering, pagination bounds",
    "list_batches": "filter predicates and ordering",
    "list_schedules": "ordering",
    "get_attempts": "per-job ordering (ORDER BY attempt)",
    "get_events": "per-job ordering (ORDER BY occurred_at)",
    "poll_reclaim_events": "watermark predicate + ORDER BY id + LIMIT",
    "poll_cancel_flags": "selection over cancel-requested rows",
    "reclaim_expired_locks": "sweep predicate + LIMIT + retry/exhausted split",
    "deadline_sweep": "sweep predicate + LIMIT",
    "scheduled_to_pending": "sweep predicate + LIMIT",
    "prune_old_batches": "retention predicate + LIMIT",
    "enqueue_batch": "per-item dedup decisions across a batch",
    "enqueue_batch_atomic": "per-item dedup decisions across a batch",
    "enqueue_batch_fast": "per-item dedup decisions across a batch",
    "count_active_jobs": "aggregate over a selected set",
    "count_pending_jobs": "aggregate over a selected set",
    "count_batch_non_terminal": "aggregate over a selected set",
    "extend_reservation_leases": "selection of the worker's held slots",
    "heartbeat_jobs": "selection of the worker's running jobs",
    "retry_job": "eligibility predicate",
    "abort_batch": "selection of the batch's non-terminal members",
}

#: Seams that cannot diverge semantically, with the reason. A method here is
#: claiming "there is no selection or ordering decision in this seam".
_NO_SEMANTIC_SURFACE: dict[str, str] = {
    # Keyed single-row reads/writes: the caller names the row, so there is
    # no selection decision either backend could make differently.
    "get": "caller supplies the job id; no selection or ordering",
    "get_batch": "caller supplies the batch id; no selection or ordering",
    "get_actor_max_pending": "caller supplies the actor; scalar return",
    "enqueue": "caller supplies the row; dedup predicates pinned separately",
    "enqueue_with_conn": "caller supplies the row and the connection",
    "mark_succeeded": "keyed single row named by job id",
    "mark_succeeded_with_conn": "keyed single row named by job id",
    "mark_failed_or_retry": "keyed single row named by job id",
    "mark_cancelled": "keyed single row named by job id",
    "mark_abandoned": "keyed single row named by job id",
    "mark_snoozed": "keyed single row named by job id",
    "mark_retry_after": "keyed single row named by job id",
    "write_attempt": "caller supplies the attempt row",
    "write_cancel_request": "keyed single row named by job id",
    "write_cancel_escalation": "keyed single row named by job id",
    "create_batch": "caller supplies the batch row",
    "complete_batch": "keyed single batch row",
    "increment_batch_failures": "keyed single batch row",
    "reset_batch_failures": "keyed single batch row",
    "create_schedule": "caller supplies the schedule row",
    "update_schedule": "keyed single schedule row",
    "delete_schedule": "keyed single schedule row",
    "subscribe_wake": "notification plumbing; returns no rows",
    "subscribe_cancel_wake": "notification plumbing; returns no rows",
}


def _public_methods(cls: type) -> dict[str, Callable[..., Any]]:
    """Public callables defined on *cls* (or inherited), excluding dunders
    and private helpers."""
    out: dict[str, Callable[..., Any]] = {}
    for name, member in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        if not callable(member):
            continue
        out[name] = member
    return out


def test_every_shared_seam_is_classified_for_semantic_parity() -> None:
    """A seam present on BOTH backends is either pinned by a parity test or
    registered as having no selection/ordering surface.

    This is the completeness half. It walks the intersection of the two
    backends' public surfaces, so the next seam that can answer differently
    in the mirror than in production fails here on arrival — rather than
    passing every aliasing guard and being discovered in a production
    incident, which is how all three known instances were found.
    """
    mem = _public_methods(InMemoryBackend)
    pg = _public_methods(PostgresBackend)
    shared = sorted(set(mem) & set(pg))

    classified = set(_SEMANTIC_SEAMS) | set(_SEMANTIC_SEAMS_UNPINNED) | set(_NO_SEMANTIC_SURFACE)
    unclassified = [name for name in shared if name not in classified]

    assert not unclassified, (
        "Unclassified backend seam(s) — each can silently answer differently "
        "in InMemoryBackend than in PostgresBackend, and no existing guard "
        "would notice:\n  "
        + "\n  ".join(unclassified)
        + "\n\nIf the seam selects or orders rows, add a behavioural parity "
        "test (see tests/test_in_memory_dispatch_parity.py) and register it "
        "in _SEMANTIC_SEAMS. If it cannot diverge, register it in "
        "_NO_SEMANTIC_SURFACE with the reason."
    )


def test_registered_parity_pins_exist() -> None:
    """Every seam registered as pinned names a parity test file that exists.

    A registry entry pointing at a deleted file is a guard that silently
    stopped guarding — the failure mode this whole file exists to prevent,
    reproduced one level up.
    """
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent
    missing = [
        f"{seam} -> {path}"
        for seam, path in _SEMANTIC_SEAMS.items()
        if not (repo_root / path).is_file()
    ]

    assert not missing, (
        "Registered parity pin(s) name a file that does not exist:\n  "
        + "\n  ".join(missing)
        + "\n\nEither restore the parity test or re-classify the seam."
    )


def test_no_seam_is_registered_twice() -> None:
    """A seam cannot be both pinned and exempt.

    Both registries are edited by hand under review pressure; an entry that
    drifts into both reads as covered from either side while the behavioural
    pin may have been dropped.
    """
    pinned = set(_SEMANTIC_SEAMS)
    unpinned = set(_SEMANTIC_SEAMS_UNPINNED)
    exempt = set(_NO_SEMANTIC_SURFACE)
    overlap = sorted((pinned & exempt) | (unpinned & exempt) | (pinned & unpinned))
    assert not overlap, "Seam(s) registered in more than one registry: " + ", ".join(overlap)


def test_no_confirmed_divergence_lingers_unpinned() -> None:
    """A CONFIRMED DIVERGENT seam cannot sit in ``_SEMANTIC_SEAMS_UNPINNED``.

    The unpinned registry holds unaudited seams — places that MIGHT answer
    differently, nobody has checked. A divergence someone has confirmed is
    past prose: it is fixed and pinned, or its parity pin sits red in
    ``_SEMANTIC_SEAMS`` until the fix lands. Parking it as an unpinned note
    reads as a TODO nobody owes, and parking it in ``_NO_SEMANTIC_SURFACE``
    would assert the opposite of what the code does — the failure mode
    that let the sweepaudit registry justify an unbounded statement on a
    bound that does not hold.
    """
    lingering = {
        seam: note
        for seam, note in _SEMANTIC_SEAMS_UNPINNED.items()
        if "CONFIRMED DIVERGENT" in note
    }
    assert not lingering, (
        "Confirmed-divergent seam(s) parked unpinned — a confirmed "
        "divergence owes a behavioural parity test (it may sit red until "
        "the fix lands), registered in _SEMANTIC_SEAMS:\n  "
        + "\n  ".join(f"{seam}: {note}" for seam, note in lingering.items())
    )
