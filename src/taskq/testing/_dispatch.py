"""Dispatch operations for InMemoryBackend.

``dispatch_batch`` lives here as a module-level function taking
``self: InMemoryBackend`` as the first parameter.
"""

import secrets
from collections import defaultdict as _dd
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import UUID

from taskq.backend._protocol import JobRow, QueueMode
from taskq.constants import SMALLINT_MAX
from taskq.testing._reads import _read_copy

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = ["_dispatch_batch", "_set_queue_mode"]

# Mirrors WorkerSettings.dispatch_oversample's default (settings.py): each
# per-(actor, queue) candidate read is bounded by residual * oversample —
# the truncation the differential must model, because a dispatchable job
# sorted behind a deep blocked cohort that fills the truncated read is
# PG's honest observable (the round dispatches NOTHING, never "past" the
# truncation).
_DISPATCH_OVERSAMPLE: Final[int] = 2


async def _dispatch_batch(
    self: "InMemoryBackend",
    worker_id: UUID,
    queues: list[str],
    limit: int,
    lock_lease: timedelta,
) -> list[JobRow]:
    now = self._clock.now()

    running_per_actor: dict[str, int] = {}
    running_identities: set[tuple[str, str]] = set()
    for row in self._jobs.values():
        if row.status == "running":
            running_per_actor[row.actor] = running_per_actor.get(row.actor, 0) + 1
            if row.identity_key is not None:
                running_identities.add((row.actor, row.identity_key))

    # Why: `row.queue in queues` with NO `not queues` escape — PG builds the
    # candidate set with ``CROSS JOIN LATERAL unnest((SELECT queues FROM
    # params))`` (backend/_dispatch_sql.py), and an empty array annihilates
    # every candidate: ``queues=[]`` means match NOTHING. The old
    # ``not queues or ...`` read the same input as "no filter" (match ALL),
    # so the mirror dispatched work a real worker polling the same empty
    # list never would — the mirror was greener than production.
    #
    # Why: `or []` cannot re-admit an empty queue list here — the candidate
    # filter above matches NOTHING for `[]`, so no row survives to be
    # round-robin-ordered; it is only a None guard, never a selection.
    _use_round_robin = any(self._queues.get(q) == "round_robin" for q in (queues or []))

    # ── per_actor_capacity + repend_capacity + candidates laterals ────
    # Candidates come FROM the actor_config registry, exactly PG's
    # per_actor_capacity and repend_capacity CTEs
    # (backend/_dispatch_sql.py): zero registered actors means zero
    # capacity rows means zero candidates — "no actors registered" must
    # never read as "no filter" (the mirror was greener than
    # production). residual = the round's limit when the actor has no
    # max_concurrent, else max(max_concurrent - in_flight, 0).
    #
    # Routing contract, mirroring PG's two candidate arms: a pending row
    # routes by its OWN queue label while never claimed
    # (started_at IS NULL — producer placement governs, so post-move
    # strays and enqueue overrides keep their queue), and by the actor's
    # CURRENT stored assignment once claimed (started_at IS NOT NULL —
    # every re-pend path keeps the row's label as audit trail but
    # follows the assignment, so a move's running-job tail drains
    # through the target queue's consumers). started_at is the durable
    # "was claimed" marker: dispatch stamps it and no re-pend path on
    # either backend clears it (attempt is NOT a marker — the
    # snooze/refund arms give the claim's increment back).
    # A fresh ordering of the registered actors per round, mirroring PG's
    # actor_rotation CTE (backend/_dispatch_sql.py): least-loaded first,
    # broken by a fresh random draw. Without it the round's cut is a
    # stable total order over every actor's head job, so the same prefix
    # of actors wins every round and the rest never run. The key is per
    # ACTOR, so an actor's own jobs keep their exact priority order; only
    # which actors win the contested slots rotates.
    _rotation: dict[str, tuple[int, float]] = {
        _actor: (
            running_per_actor.get(_actor, 0),
            secrets.randbelow(1 << 52) / float(1 << 52),
        )
        for _actor in self._actor_configs_meta
    }

    candidates: list[JobRow] = []
    _fairness_rank: dict[UUID, int] = {}
    for _actor, _cfg in self._actor_configs_meta.items():
        _cap = _cfg.max_concurrent
        _residual = limit if _cap is None else max(_cap - running_per_actor.get(_actor, 0), 0)
        if _residual <= 0:
            continue
        _bound = _residual * _DISPATCH_OVERSAMPLE
        _by_queue: dict[str, list[JobRow]] = _dd(list)
        _repended_by_fk: dict[str, list[JobRow]] = _dd(list)
        for row in self._jobs.values():
            if not (
                row.status == "pending"
                and row.actor == _actor
                and row.scheduled_at <= now
                and (row.schedule_to_close is None or row.schedule_to_close > now)
            ):
                continue
            if row.started_at is None:
                # Label-routed arm: PG's per_actor_capacity x
                # unnest(queues) probes, queue label against the
                # subscription.
                if row.queue in queues:
                    _by_queue[row.queue].append(row)
            elif _cfg.queue in queues:
                # Assignment-routed arm: PG's repend_capacity gate (the
                # actor's stored assignment against the subscription) —
                # the row's own label is irrelevant once claimed.
                _fk = row.fairness_key if row.fairness_key is not None else "__null__"
                _repended_by_fk[_fk].append(row)
        for _queue_rows in _by_queue.values():
            if _use_round_robin:
                _fk_groups: dict[str, list[JobRow]] = _dd(list)
                for r in _queue_rows:
                    # Why: ONE shared "__null__" partition for every unkeyed
                    # job, exactly PG's ``PARTITION BY COALESCE(j2.fairness_key,
                    # '__null__')`` (backend/_dispatch_sql.py). The old
                    # per-row synthetic partition (f"__null__{r.id}") ranked
                    # every unkeyed job at fairness_rank 1, so a bounded batch
                    # was consumed entirely by the unkeyed cohort and the keyed
                    # cohorts starved — the exact round-robin starvation the
                    # mode exists to prevent, in the default configuration
                    # (fairness_key is None by default). Unkeyed jobs rank
                    # 1, 2, 3… and yield their surplus slots to the keyed
                    # cohorts.
                    fk = r.fairness_key if r.fairness_key is not None else "__null__"
                    _fk_groups[fk].append(r)
                for _fk_rows in _fk_groups.values():
                    _fk_rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
                    # The oversample bound is per fairness partition (a global
                    # LIMIT would truncate before partitioning — PG filters
                    # ``fairness_rank <= residual * oversample`` instead), so
                    # every cohort contributes candidates up to the bound.
                    for _rank, _r in enumerate(_fk_rows, 1):
                        if _rank <= _bound:
                            _fairness_rank[_r.id] = _rank
                            candidates.append(_r)
            else:
                # Strict-FIFO lateral: ORDER BY priority DESC, scheduled_at,
                # id LIMIT residual * oversample — the truncation that can
                # starve a dispatchable job sorting behind the bound.
                _queue_rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
                candidates.extend(_queue_rows[:_bound])
        # Assignment-routed admission, mirroring PG's rr_tail_keys
        # per-cohort probes: the same per-cohort residual * oversample
        # bound (a queue-agnostic global bound would truncate before the
        # cohort partition, exactly the preemption the label-routed arm's
        # comment above describes), one rank series per cohort. In
        # round-robin mode the ranks are real, so a re-pended row takes
        # its cohort turn beside never-claimed rows; a cohort carrying
        # both populations has two rank series that interleave by
        # priority downstream, never starving either. In strict-FIFO
        # mode the rank is inert (that variant ranks by priority alone),
        # matching PG's NULL::bigint fairness_rank on its arm.
        for _fk_rows in _repended_by_fk.values():
            _fk_rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
            for _rank, _r in enumerate(_fk_rows[:_bound], 1):
                if _use_round_robin:
                    _fairness_rank[_r.id] = _rank
                candidates.append(_r)

    # ── identity_dedup, BEFORE ranking ──────────────────────────────────
    # PG dedupes candidates per (actor, identity_key) before pending_rank
    # is computed (the identity_dedup CTE): the best candidate per identity
    # (priority DESC, scheduled_at, id), with identities a running job
    # already holds dropped entirely; identity-less rows pass through.
    _deduped: list[JobRow] = []
    _best_by_identity: dict[tuple[str, str], JobRow] = {}
    for r in candidates:
        if r.identity_key is None:
            _deduped.append(r)
            continue
        _ident = (r.actor, r.identity_key)
        if _ident in running_identities:
            continue
        _cur = _best_by_identity.get(_ident)
        if _cur is None or (-r.priority, r.scheduled_at, r.id) < (
            -_cur.priority,
            _cur.scheduled_at,
            _cur.id,
        ):
            _best_by_identity[_ident] = r
    _deduped.extend(_best_by_identity.values())

    # ── ranked + eligible ordering ──────────────────────────────────────
    # pending_rank: per-actor row number over the deduped set, per mode
    # (PG's ranked CTE — strict FIFO: priority DESC, scheduled_at, id;
    # round_robin: fairness_rank, priority DESC, scheduled_at, id). The
    # final selection order is PG's eligible ORDER BY: pending_rank, then
    # fairness_rank (round_robin only; strict-FIFO rows carry none), then
    # priority DESC, scheduled_at, id — NEVER alphabetical actor order,
    # which the old per-rank interleave substituted whenever a round's
    # limit cut inside a rank shared by jobs of different actors.
    _ranked_by_actor: dict[str, list[JobRow]] = _dd(list)
    for c in _deduped:
        _ranked_by_actor[c.actor].append(c)
    _pending_rank: dict[UUID, int] = {}
    for _actor_rows in _ranked_by_actor.values():
        if _use_round_robin:
            _actor_rows.sort(
                key=lambda r: (_fairness_rank[r.id], -r.priority, r.scheduled_at, r.id)
            )
        else:
            _actor_rows.sort(key=lambda r: (-r.priority, r.scheduled_at, r.id))
        for _rank, _r in enumerate(_actor_rows, 1):
            _pending_rank[_r.id] = _rank
    candidates = sorted(
        _deduped,
        key=lambda r: (
            _pending_rank[r.id],
            _fairness_rank[r.id] if _use_round_robin else 0,
            -r.priority,
            _rotation[r.actor],
            r.scheduled_at,
            r.id,
        ),
    )

    dispatched_per_actor: dict[str, int] = {}
    newly_dispatched_identities: set[tuple[str, str]] = set()
    dispatched: list[JobRow] = []

    for row in candidates:
        if len(dispatched) >= limit:
            break

        # Every candidate's actor carries an actor_config row by
        # construction (candidates come FROM the registry), exactly as
        # PG's eligible_candidates LEFT JOIN always finds its row.
        cap = self._actor_configs_meta[row.actor].max_concurrent

        per_dispatch_cap = cap if cap is not None else limit
        if dispatched_per_actor.get(row.actor, 0) >= per_dispatch_cap:
            continue

        if cap is not None:
            in_flight = running_per_actor.get(row.actor, 0) + dispatched_per_actor.get(row.actor, 0)
            if in_flight >= cap:
                continue

        if row.identity_key is not None:
            ident = (row.actor, row.identity_key)
            if ident in running_identities or ident in newly_dispatched_identities:
                continue
            newly_dispatched_identities.add(ident)

        dispatched_per_actor[row.actor] = dispatched_per_actor.get(row.actor, 0) + 1

        updated = replace(
            row,
            status="running",
            locked_by_worker=worker_id,
            lock_expires_at=now + lock_lease,
            started_at=now,
            finished_at=None,
            last_heartbeat_at=now,
            error_class=None,
            error_message=None,
            error_traceback=None,
            result=None,
            result_size_bytes=None,
            # Saturating, mirroring PG's LEAST(attempt + 1, ...) clamp:
            # the attempt column is a smallint there, so an
            # indefinite-retry job that reaches the ceiling must pin
            # rather than make the claim statement raise and abort the
            # whole batch alongside it.
            attempt=min(row.attempt + 1, SMALLINT_MAX),
        )
        self._jobs[row.id] = updated
        self._append_state_change_event(
            job_id=row.id,
            from_state="pending",
            to_state="running",
            now=now,
            worker_id=worker_id,
        )
        dispatched.append(_read_copy(updated))

    return dispatched


def _set_queue_mode(self: "InMemoryBackend", queue_name: str, mode: QueueMode) -> None:
    from taskq.testing._runner import set_queue_mode as _set_queue_mode_impl

    _set_queue_mode_impl(self, queue_name, mode)
