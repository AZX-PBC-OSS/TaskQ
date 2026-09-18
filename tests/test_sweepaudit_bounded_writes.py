"""Bounded-write audit: every module-level UPDATE/DELETE in the package is
either bounded by a ``LIMIT`` or registered here with the reason it cannot
grow with the backlog.

The class this file guards: work proportional to an unbounded backlog
inside one transaction. Past ~2 s of open transaction the reclaim event
watermark (``RECLAIM_EVENT_VISIBILITY_DELAY``) is silently corrupted; past
the iteration deadline the transaction rolls back whole and the backlog
ratchets — the failure that filed as "jobs stuck, fleet reports healthy".
Every site of that class was bounded (LIMIT inside a windowing CTE,
per-batch commit through ``_drain_bounded``, breaker-wrapped via
``PostgresBackend._run_bounded_sweep``), and each fixed site has its own
dynamic statement-count test. Those tests guard their site. This file
guards the *surface*: it walks every module in the package, so a NEW
unbounded write statement — the tenth site — fails here on arrival, and a
cleared-by-audit statement stays cleared because this registry says so,
not because someone once read it.

Scope, stated plainly:

* Module-level string constants only. Function-local SQL (the
  ``_cancel_bulk.py`` batch statements, the cron tick's inline UPDATEs) is
  bounded by construction at its call site and pinned by the dynamic
  bounded tests (``tests/test_cancel_where_bounded.py``,
  ``tests/test_cron_tick_bounded.py``).
* The statement shape, not its effectiveness. That a LIMIT is fenced
  (MATERIALIZED) and actually bounds rows is pinned per site by the
  dynamic tests; this file pins that the bound *exists* or that its
  absence is deliberate — including, for the windowed write statements,
  that the MATERIALIZED fence exists: an unfenced LIMIT-ed CTE is not a
  bound on the rows the data-modifying statement touches, so the fence
  is part of "the bound exists", while that the fence actually holds
  rows stays with the per-site dynamic tests.
* This asserts implementation surface by design — the precedent is
  ``tests/test_sweepaudit_dispatch_bound.py``: pin the production
  constant, not a copy, because a copy drifts from the SQL that actually
  runs. A rename breaking this test is the guard noticing change in the
  exact surface it watches; the failure message says what to do.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import re

import pytest

import taskq
from taskq.backend._batch_sql import (  # pyright: ignore[reportPrivateUsage]  # Why: pinning the exact production statement is the point; redefining it here would let the pin drift from the SQL that runs.
    _PRUNE_OLD_BATCHES_SQL,
)
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _SWEEP_4_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
)
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _ARCHIVE_CTE_ACTOR_SQL,
    _ARCHIVE_CTE_SQL,
    _EXPIRY_CTE_SQL,
)

_WRITE_RE = re.compile(r"\b(UPDATE|DELETE)\b")

# A real LIMIT clause keyword, not a substring: ``rate_limit_buckets``
# contains "LIMIT" inside the identifier, and a plain substring test
# both waves unregistered statements on that table through as bounded
# and flags properly-registered ones as stale.
_LIMIT_CLAUSE_RE = re.compile(r"\bLIMIT\b", re.IGNORECASE)

# Unbounded write statements that are deliberate: each entry is the
# constant name mapped to (scoping substring that must survive in the
# body, why the row count cannot grow with the jobs backlog). An entry is
# a claim with a tripwire: widen or remove the scoping predicate and the
# substring check fails; bound the statement properly and the staleness
# check fails until the entry is removed. Registering a new entry requires
# writing the justification — that sentence is the review.
_EXEMPT: dict[str, tuple[str, str]] = {
    # ── Keyed single-row writes: the predicate names one primary key ──
    "_INCREMENT_BATCH_FAILURES_SQL": (
        "WHERE id = $1",
        "keyed single batch row",
    ),
    "_RESET_BATCH_FAILURES_SQL": (
        "WHERE id = $1",
        "keyed single batch row",
    ),
    "_ABORT_BATCH_ROW_SQL": (
        "WHERE id = $1",
        "keyed single batch row",
    ),
    "_COMPLETE_BATCH_SQL": (
        "WHERE id = $1",
        "keyed single batch row",
    ),
    "_SCHEDULE_DELETE_SQL": (
        "WHERE id = $1",
        "keyed single cron_schedules row",
    ),
    "_SCHEDULE_UPDATE_SQL": (
        "cron_schedules SET",
        "prefix constant; the sole call site completes it with "
        "'WHERE id = $1 RETURNING *' — keyed single cron_schedules row",
    ),
    "_SCHEDULE_ENABLE_SQL": (
        "WHERE id = $1",
        "keyed single cron_schedules row (admin)",
    ),
    "_SCHEDULE_DISABLE_SQL": (
        "WHERE id = $1",
        "keyed single cron_schedules row (admin)",
    ),
    "_SCHEDULE_SKIP_SQL": (
        "WHERE id = $1",
        "keyed single cron_schedules row (admin)",
    ),
    "_MOVE_LOCK_ASSIGNMENT_SQL": (
        "WHERE actor = $1",
        "keyed single actor_config row (move_actor_queue's assignment lock)",
    ),
    "_MOVE_SET_ASSIGNMENT_SQL": (
        "WHERE actor = $1",
        "keyed single actor_config row (move_actor_queue's assignment flip)",
    ),
    "CANCEL_ESCALATION_SQL": (
        "WHERE id = $1",
        "keyed single running job, escalated on cancel request",
    ),
    "UPDATE_WORKER_LIVENESS_SQL_TEMPLATE": (
        "WHERE id = $1",
        "keyed single workers row (own heartbeat)",
    ),
    "_LEADER_ELECT_SQL_TEMPLATE": (
        "ON CONFLICT (singleton)",
        "keyed singleton maintenance_leader row: the table's primary key is a "
        "boolean CHECKed to true, so the insert and its conflict update each "
        "touch at most that one row",
    ),
    "_LEADER_RENEW_SQL_TEMPLATE": (
        "WHERE singleton = true AND worker_id = $1 AND elected_at = $2",
        "keyed singleton maintenance_leader row, fenced on the holder's term",
    ),
    "_LEADER_RESIGN_SQL_TEMPLATE": (
        "WHERE singleton = true AND worker_id = $1 AND elected_at = $2",
        "keyed singleton maintenance_leader row, fenced on the holder's term",
    ),
    "_ISOLATE_JOB_SQL_TEMPLATE": (
        "WHERE j.id = $1",
        "keyed single running job (watchdog self-isolation)",
    ),
    "_RELEASE_FENCED_SQL_TEMPLATE": (
        "WHERE bucket_name",
        "keyed single reservation_slots row (bucket + slot + holder fence)",
    ),
    "_RELEASE_SQL_TEMPLATE": (
        "WHERE bucket_name",
        "keyed single reservation_slots row (bucket + slot + holder)",
    ),
    "_SET_ACTOR_CONFIG_CAPACITY_SQL": (
        "WHERE actor = $1",
        "keyed single actor_config row",
    ),
    "_DEREGISTER_DELETE_ACTOR_CONFIG_SQL": (
        "WHERE actor = $1",
        "keyed single actor_config row",
    ),
    "_DEREGISTER_PURGE_QUEUE_SQL": (
        "WHERE name = $1",
        "keyed single queues row, deleted only when provably unused",
    ),
    "_UPSERT_ACTOR_CONFIG_SQL": (
        "ON CONFLICT",
        "INSERT ... ON CONFLICT over the registered-actor set; actor_config "
        "cardinality is configuration, not backlog",
    ),
    # ── Config-cardinality tables: the table itself cannot grow with the
    #    jobs backlog ──
    "_SYNC_DELETE_SQL_TEMPLATE": (
        "WHERE bucket_name = $1",
        "one bucket's reservation_slots rows; slot count is configured capacity",
    ),
    "_ENSURE_SLOTS_SQL_TEMPLATE": (
        "ON CONFLICT (bucket_name, slot_index)",
        "one keyed-materialised bucket's reservation_slots rows; row count "
        "is the configured slots capacity, and the conflict arm writes "
        "only the keyed marker (never holder or lease state)",
    ),
    "_RECLAIM_SLICE_DELETE_SQL_TEMPLATE": (
        "bucket_name = ANY($1)",
        "a bounded slice of evicted keyed buckets' idle rows; count "
        "bounded by slice size x configured slots",
    ),
    "_RECLAIM_RATE_LIMIT_SLICE_DELETE_SQL_TEMPLATE": (
        "bucket_name = ANY($1)",
        "a bounded slice of evicted keyed buckets' published "
        "rate_limit_buckets rows; count bounded by the drain's slice "
        "size x one row per bucket",
    ),
    "_DEREGISTER_DISABLE_SCHEDULES_SQL": (
        "WHERE actor = $1",
        "one actor's cron_schedules rows; schedule count per actor is configuration",
    ),
    # ── Worker-scoped: one worker's in-flight set, bounded by its own
    #    configured concurrency ──
    "UPDATE_JOBS_LOCK_SQL_TEMPLATE": (
        "WHERE locked_by_worker = $1 AND status = 'running'",
        "one worker's running jobs (heartbeat lease renew); bounded by "
        "that worker's concurrency, not the backlog",
    ),
    "UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE": (
        "WHERE locked_by_worker = $1 AND status = 'running'",
        "one worker's running jobs (the heartbeat loop's threshold-gated "
        "lease renewal, #227 — the same worker-scoped set as "
        "UPDATE_JOBS_LOCK_SQL_TEMPLATE with a narrower predicate, so the "
        "bound is at most that statement's)",
    ),
    "UPDATE_RESERVATION_LEASES_SQL_TEMPLATE": (
        "locked_by_worker = $1",
        "reservation leases for one worker's running jobs; same bound as "
        "UPDATE_JOBS_LOCK_SQL_TEMPLATE",
    ),
    # ── Batch-scoped ──
    "_ABORT_BATCH_JOBS_SQL": (
        "WHERE metadata @> $1::jsonb",
        "one batch's job membership; cleared by the class audit "
        "(abort_batch refuted as a backlog-proportional site)",
    ),
}


def _discover_write_statements() -> dict[str, str]:
    """Walk every module under ``taskq`` and return {qualified_name: body}
    for module-level string constants containing UPDATE or DELETE.

    Re-exports (``taskq.backend.postgres`` re-exporting
    ``taskq.backend._sweeps`` constants, etc.) resolve to the same str
    object and are deduplicated by identity.

    Modules whose optional dependencies are missing (``contrib.prometheus``
    and friends) are skipped rather than failing the walk: CI legs that
    install every extra run the complete walk, so a module skipped here on
    a partial-extra leg is still audited there. An import failure is not
    silently swallowed either — the known-guarded shapes (the extras'
    documented ImportErrors) are skipped; anything else re-raises.
    """
    found: dict[str, str] = {}
    seen_ids: set[int] = set()
    modules = [taskq]
    for info in pkgutil.walk_packages(taskq.__path__, prefix="taskq."):
        try:
            modules.append(importlib.import_module(info.name))
        except ImportError as exc:
            # Optional-extra guards raise ImportError with install
            # instructions at import time (contrib.prometheus, aad, vault,
            # aws, saml). A leg without the extra cannot audit those
            # modules' constants; the --all-extras legs cover them.
            known_extras = ("taskq[",)
            if exc.args and isinstance(exc.args[0], str) and exc.args[0].startswith(known_extras):
                continue
            raise
    for mod in modules:
        for name, val in inspect.getmembers(mod, lambda v: isinstance(v, str)):
            if name.startswith("__") or not _WRITE_RE.search(val) or id(val) in seen_ids:
                continue
            seen_ids.add(id(val))
            found[f"{mod.__name__}:{name}"] = val
    return found


@pytest.mark.fastapi
def test_every_write_statement_is_bounded_or_registered() -> None:
    """The guard against the tenth site: any unbounded UPDATE/DELETE that
    is not registered in ``_EXEMPT`` fails here.

    If you arrived from that failure: a write whose row count can grow
    with the jobs backlog must go through the shared bounded machinery —
    ``PostgresBackend._run_bounded_sweep`` (breaker + SET LOCAL
    statement_timeout) and ``_drain_bounded`` (per-batch commit) — or
    carry its own LIMIT inside a windowing CTE. If the statement truly
    cannot grow with the backlog (keyed, config-cardinality, or
    worker-scoped), register it in ``_EXEMPT`` above *with the reason*;
    that reason is the review — maintenance and background
    work is bounded per transaction.
    """
    unregistered: list[str] = []
    wrong_scope: list[str] = []
    for qualified, body in _discover_write_statements().items():
        if _LIMIT_CLAUSE_RE.search(body):
            continue
        name = qualified.rsplit(":", 1)[1]
        entry = _EXEMPT.get(name)
        if entry is None:
            unregistered.append(qualified)
        elif entry[0] not in body:
            wrong_scope.append(f"{qualified} (missing scoping substring {entry[0]!r})")
    assert not unregistered, (
        "Unbounded UPDATE/DELETE with no LIMIT and no registry entry:\n  "
        + "\n  ".join(unregistered)
        + "\nBound it (LIMIT in a windowing CTE via the shared bounded-sweep "
        "machinery) or register it in _EXEMPT with the reason it cannot "
        "grow with the backlog."
    )
    assert not wrong_scope, (
        "Registered exemptions whose scoping predicate no longer matches "
        "— the statement was widened or rewritten; re-review the exemption:\n  "
        + "\n  ".join(wrong_scope)
    )


@pytest.mark.fastapi
def test_exemption_registry_has_no_stale_entries() -> None:
    """The reverse direction: every registry entry must still name a real,
    still-unbounded statement.

    Bounding an exempt statement is the goal — but then the entry is
    stale, and leaving it teaches the next reader the registry is not
    maintained. Delete the entry when you land the bound; this test is the
    reminder. A renamed or deleted constant fails here too, which is how
    the registry tracks the surface it claims to cover.
    """
    discovered = _discover_write_statements()
    by_name = {q.rsplit(":", 1)[1]: (q, v) for q, v in discovered.items()}
    stale: list[str] = []
    for name in _EXEMPT:
        found = by_name.get(name)
        if found is None:
            stale.append(f"{name}: no such statement discovered")
        elif _LIMIT_CLAUSE_RE.search(found[1]):
            stale.append(f"{name} ({found[0]}): now carries a LIMIT — remove the entry")
    assert not stale, "Stale _EXEMPT entries:\n  " + "\n  ".join(stale)


# The windowed write statements this file's LIMIT walk already covers;
# named here so the fence guard below pins the exact production constants
# (the precedent of ``tests/test_sweepaudit_dispatch_bound.py``: pin the
# constant, not a copy, because a copy drifts from the SQL that runs).
_WINDOWED_WRITE_STATEMENTS: dict[str, str] = {
    "_ARCHIVE_CTE_SQL": _ARCHIVE_CTE_SQL,
    "_ARCHIVE_CTE_ACTOR_SQL": _ARCHIVE_CTE_ACTOR_SQL,
    "_EXPIRY_CTE_SQL": _EXPIRY_CTE_SQL,
    "_SWEEP_4_SQL": _SWEEP_4_SQL,
    "_SWEEP_IDLE_KEYED_BUCKETS_SQL": _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    "_SWEEP_IDLE_KEYED_SLOTS_SQL": _SWEEP_IDLE_KEYED_SLOTS_SQL,
    "_PRUNE_OLD_BATCHES_SQL": _PRUNE_OLD_BATCHES_SQL,
}


def test_windowed_write_ctes_carry_the_materialized_fence() -> None:
    """Every LIMIT-ed candidate window feeding a data-modifying statement
    carries ``AS MATERIALIZED``.

    Without the fence the planner may inline the LIMIT-ed CTE into the
    UPDATE/INSERT/DELETE that joins it and move more rows than the LIMIT —
    the LIMIT then bounds only the CTE's inlined appearances, not the
    written result, so the statement is unbounded in exactly the way this
    file exists to prevent while *looking* bounded (the LIMIT walk above
    passes it). The dynamic per-site tests pin that the fence actually
    holds; this pin exists so a new windowed write statement, or a rewrite
    that drops the keyword from one, fails on arrival at the same place
    the LIMIT itself does.
    """
    unfenced = {
        name: sql
        for name, sql in _WINDOWED_WRITE_STATEMENTS.items()
        if "AS MATERIALIZED" not in sql
    }
    assert not unfenced, (
        "LIMIT-ed candidate windows without the MATERIALIZED fence — the "
        "planner may inline them into the data-modifying statement and move "
        "more rows than the LIMIT:\n  " + "\n  ".join(unfenced)
    )
