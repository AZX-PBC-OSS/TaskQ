"""The refund-representation x cancel-seat evidence matrix (the scope the
round-3 ruling filed as ``FOLLOW-UP-WF-CANCEL-REFUND-MATRIX``, folded into
#718 by user instruction — there is no follow-up ticket).

The mechanism ships once: `_sql_claim.current_claim_join`'s exact current-
claim association (``job_id`` + the CURRENT ``claim_epoch`` + the exact
``step_key`` + the null-safe ``map_index`` — NO attempt-equality leg, NO
park-metadata test, plus the strict single-row maximality probe) shared by
the operator cancel, the fail-closed peer cancel, and the §5.4 fence. What
this module adds is the EVIDENCE BREADTH: every refund representation,
driven through its REAL product path, cancelled through BOTH seats, with
the decoy estate at full strength and the derived-capacity arithmetic
asserted independently of the product's own observation.

The seven representations and the resting estate each product path leaves:

1. ``snooze`` — the real ``defer_row_law`` (an ordinary ``Snooze``): the row
   rests ``scheduled`` with the displayed ordinal REFUNDED (A-1 = 0), the
   epoch unchanged, the outer current claim ``awaited`` at the recorded
   attempt A = 1, no park marker.
2. ``retry_after`` — the real legacy ``RetryAfter(consume_budget=False)``
   through the catch arm's own ``deferral_disposition`` mapping (it resolves
   ``"snooze"``): the identical refunded shape.
3. ``retry_after_non_consuming`` — the real policy-classified
   ``policy_retry_defer`` route with valid declared counter/limit facts: the
   refunded shape plus the policy counter advanced exactly once.
4. ``hold`` — the real wait-site hold (``register_hold`` +
   ``mark_awaited``, the two writes a body's ``ctx.wait_signal`` makes
   before ``NodeHeldError`` unwinds): the row rests ``pending`` in the held
   representation, the outer claim ``awaited``; the attempt is NOT refunded
   (a hold is not a refund).
5. ``shutdown_drain`` — the real worker shutdown drain
   (``drain_local_queue_to_pending``, the shared ``ATTEMPT_REFUND_SQL``
   fragment): the row rests ``pending`` with the attempt refunded, the
   outer claim still ``running``; the promoted source's page-width
   reservation SURVIVES the drain (the drain clears the lock, never the
   metadata), so the cancel's reservation removal is exercised non-
   vacuously.
6. ``heartbeat_reconcile`` — the real ``heartbeat_loop`` tick's claim-loss
   reconcile: the row STAYS ``running`` (Sweep 1 owns the reclaim) with the
   attempt refunded and ``started_at`` un-stamped, the outer claim still
   ``running``. Its OPERATOR cell is the cooperative arm BY DESIGN (the
   ruling's own B-5 refinement: phase 1 never terminalizes a running row,
   writes no terminal event, and retains the reservation — the later actual
   terminal writer records the running-to-terminal event); its PEER cell is
   the full terminal transition.
7. ``reclaim_crashed`` — the real machinery reclaim (``repend_attempt``
   with ``machinery_crashed=True``, the ladder's reclaim seat): the row
   re-pends ``scheduled`` at the CHARGED ordinal (no refund), the outer
   claim rests ``crashed`` with the epoch preserved.

Both cancel seats per representation:
  * the operator's ``CANCEL_NODES_SQL_TEMPLATE`` — the exact current claim
    fences with ``WorkflowCancelled``;
  * the fail-closed peer cascade (``_resolve_failed_parent`` →
    ``FAIL_CLOSED_CASCADE_SQL``) — the same with ``CancelledByPeerFailure``
    and the structured ``peer_cancel`` cause preserved.

Every terminalizing cell asserts: the job terminalises ``cancelled``; the
reservation is removed where held; EXACTLY ONE ``job_events`` state_change
per winner with the TRUTHFUL captured ``from_state`` (the representation's
real resting status); the five decoys (the sibling ``ctx.substep`` row, the
loop ``.iter0`` row, the older-epoch row, the terminal-history row, the
different-``map_index`` row) keep their prior status, class, ordinal and
update instant byte-for-byte; no second disposition (no retry, no extra
terminal, no snooze/policy counter move, no ladder entry, no metadata
write beyond the terminal's own); the row is unclaimable afterwards; and
the derived capacity moves exactly once — asserted against an INDEPENDENT
raw count of the documented live-row/reservation predicate, never the
product's own counter alone."""

from __future__ import annotations

import json as _json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


def _new_uuid_str() -> str:
    return str(new_uuid())


class _Conn:
    """The observation's minimal connection view over the pool."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    async def fetchrow(self, *args: Any) -> Any:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(*args)


def _detail(raw: object) -> dict[str, object]:
    return _json.loads(raw) if isinstance(raw, str) else dict(raw)  # type: ignore[arg-type,call-overload]  # Why: the jsonb round-trip's decode boundary — the row's jsonb read is Any-typed and the decoded shape is this helper's own dict contract.


@dataclass(frozen=True, slots=True)
class Estate:
    """One representation's resting estate: the ids the cell asserts on
    plus the expected resting facts."""

    flow: JobId
    job: JobId
    worker: JobId
    outer: str
    substep: str
    iteration: str
    older: str
    terminal_history: str
    map_sibling: str
    key: str
    resting_status: str
    claim_status: str
    job_attempt: int
    claim_attempt: int
    reserved: bool
    cooperative_operator: bool
    decoys: tuple[str, ...]
    decoy_names: dict[str, str]


# ── the estate seeds ─────────────────────────────────────────────────────


async def _seed_flow_root(conn: asyncpg.Connection, schema: str) -> JobId:
    flow = JobId(_new_uuid_str())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, metadata, fairness_key) "
        "VALUES ($1, 'matrix', 'matrix', '{}', 'running', 3, 'transient', '__flow__', "
        "$2::jsonb, $3)",
        flow,
        _json.dumps({"flow_id": str(flow), "workflow": "matrix_flow"}),
        str(flow),
    )
    return flow


async def _seed_target(
    conn: asyncpg.Connection,
    schema: str,
    flow: JobId,
    key: str,
    worker: JobId,
    *,
    extra_meta: dict[str, object] | None = None,
) -> JobId:
    """The claimed-and-running target row the representation drivers start
    from: attempt 1 charged, epoch 2 current, locked, started (the drain's
    and reconcile's own premise), no job_attempts row (nothing executed)."""
    job = JobId(_new_uuid_str())
    meta: dict[str, object] = {"flow_id": str(flow)}
    if extra_meta is not None:
        meta.update(extra_meta)
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, claim_epoch, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key, "
        "locked_by_worker, lock_expires_at, started_at) "
        "VALUES ($1, 'matrix', 'matrix', '{}', 'running', 1, 3, 'transient', $2, "
        "$3::jsonb, 2, clock_timestamp(), clock_timestamp(), $4, 'workflow-run', $5, "
        # Why: the claim CAS's own lease shape (NODE_CLAIM_SQL's 90 seconds) —
        # the row reads as a LIVE claim, never a lapsed one.
        "$6, now() + interval '90 seconds', clock_timestamp())",
        job,
        key,
        _json.dumps(meta),
        str(flow),
        f"wf:{flow}:{key}",
        worker,
    )
    return job


async def _seed_decoys(
    conn: asyncpg.Connection,
    schema: str,
    estate_flow: JobId,
    job: JobId,
    key: str,
    *,
    claim_status: str,
) -> tuple[str, str, str, str, str]:
    """The FULL-STRENGTH decoy estate, all on the SAME job: the sibling
    ``ctx.substep`` row, the loop ``.iter0`` row (the loop driver's own
    iteration-claim shape — the iteration claim rides the loop node's job
    id and epoch at a distinct step key), the older-epoch row, the
    terminal-history row (the deadline re-pend's ``failed`` shape at a
    different attempt), and the different-``map_index`` row. The three
    in-domain decoys are stamped STRICTLY AFTER the outer claim's instant
    (staggered microseconds) so a broken association's maximality probe
    resolves to a decoy deterministically — the mutant red is the decoy's
    own rewrite, never a timestamp tie."""

    async def _ledger(
        step_key: str,
        map_index: int | None,
        attempt: int,
        epoch: int,
        status: str,
        error_class: str | None,
        offset_us: int,
    ) -> str:
        ledger_id = _new_uuid_str()
        await conn.execute(
            f'INSERT INTO "{schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
            "map_index, attempt, claim_epoch, status, error_class, created_at, "
            "updated_at) VALUES ($1, $2, $3, $4, $5::smallint, $6::int, $7::bigint, "
            "$8::text, $9::text, clock_timestamp(), "
            # Why: the decoys' instants are STAGGERED by whole microseconds
            # strictly after the outer claim's — a broken association's
            # maximality probe resolves to a decoy deterministically (the
            # mutant red is the decoy's own rewrite, never a timestamp tie).
            "clock_timestamp() + ($10::int * interval '1 microsecond'))",
            ledger_id,
            estate_flow,
            job,
            step_key,
            map_index,
            attempt,
            epoch,
            status,
            error_class,
            offset_us,
        )
        return ledger_id

    iteration = await _ledger(f"{key}.iter0", None, 1, 2, claim_status, None, 1)
    map_sibling = await _ledger(key, 0, 1, 2, claim_status, None, 2)
    substep = await _ledger(f"{key}.sub", None, 1, 2, claim_status, None, 3)
    older = await _ledger(key, None, 1, 1, claim_status, None, 4)
    terminal_history = await _ledger(key, None, 2, 2, "failed", "DeadlineExceeded", 5)
    return substep, iteration, older, terminal_history, map_sibling


# ── the seven representation drivers (each a REAL product path) ──────────


async def _claim_outer(
    pool: asyncpg.Pool, wf_sql: WorkflowSql, flow: JobId, job: JobId, key: str
) -> Any:
    from taskq.workflows.ledger import claim_step_ledger

    async with pool.acquire() as conn:
        return await claim_step_ledger(
            conn,
            wf_sql,
            flow_id=flow,
            job_id=job,
            step_key=key,
            map_index=None,
            attempt=1,
            claim_epoch=2,
        )


async def _drive_snooze(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.workflows.api._runner_defer import defer_row_law

    del schema, key
    outcome = await defer_row_law(
        pool,
        wf_sql,
        job_id=job,
        worker_id=worker,
        delay=timedelta(seconds=5),
        attempt=1,
        claim_epoch=2,
        disposition="snooze",
        signal_name="Snooze",
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.claimed_at,
        flow_id=flow,
    )
    assert outcome.verdict == "deferred", outcome


async def _drive_retry_after(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.exceptions import RetryAfter
    from taskq.workflows.api._runner_defer import defer_row_law, deferral_disposition

    del schema, key
    exc = RetryAfter(timedelta(seconds=5), consume_budget=False)
    # the catch arm's OWN mapping resolves the legacy signal's disposition —
    # never a hand-written disposition string.
    disposition = deferral_disposition(exc)
    assert disposition == "snooze", disposition
    outcome = await defer_row_law(
        pool,
        wf_sql,
        job_id=job,
        worker_id=worker,
        delay=exc.delay,
        attempt=1,
        claim_epoch=2,
        disposition=disposition,
        signal_name=type(exc).__name__,
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.claimed_at,
        flow_id=flow,
    )
    assert outcome.verdict == "deferred", outcome


async def _drive_non_consuming(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.workflows.api._runner_defer import policy_retry_defer

    del schema

    class FlakyProviderError(Exception):
        """A policy-classified advertised retry (the classifier's subject)."""

    async def _project_auto(
        flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, object]
    ) -> None:
        del flow_id, node_id, kind, payload  # the deferred verdict never reaches the projector

    verdict = await policy_retry_defer(
        pool,
        wf_sql,
        flow_id=flow,
        row={"id": job, "step_key": key, "map_index": None},
        worker_id=worker,
        attempt=1,
        claim_epoch=2,
        ledger_id=claim.ledger_id,
        ledger_claimed_at=claim.claimed_at,
        exc=FlakyProviderError("the provider advertised a retry"),
        delay=timedelta(seconds=5),
        capture_policy=None,
        project_auto=_project_auto,
    )
    assert verdict == "deferred", verdict


async def _drive_hold(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.workflows.api._hitl import mark_awaited, register_hold

    del worker
    async with pool.acquire() as conn:
        # the wait site's two writes: the ledger's resume-not-retry face,
        # then the held representation (pending + the deadline + the
        # signal row) — exactly what a body's ``ctx.wait_signal`` mints
        # before ``NodeHeldError`` unwinds the body.
        await mark_awaited(conn, schema, claim.ledger_id)
        await register_hold(
            conn,
            schema=schema,
            workflow_id=flow,
            node_id=job,
            node_key=key,
            signal_name="approval",
            hold_epoch=1,
            call_id="call:1:0",
            payload_schema={
                "type": "object",
                "properties": {"verdict": {"type": "string"}},
                "required": ["verdict"],
            },
            timeout_s=timedelta(seconds=60),
            map_index=None,
            # the driving claim's own epoch (the seeded target row is running at 2)
            # — the mint's park fences on it.
            claim_epoch=2,
        )


async def _drive_shutdown_drain(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.worker.shutdown import drain_local_queue_to_pending
    from tests.test_rt_458_claim_loss_attempt_charge import _heartbeat_deps

    del wf_sql, flow, job, key, claim
    deps = _heartbeat_deps(_dsn_by_schema[schema], schema, pool)
    drained = await drain_local_queue_to_pending(deps, worker)
    assert drained == 1, f"the drain must hand the claimed row back: {drained}"


async def _drive_heartbeat_reconcile(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from tests.test_rt_458_claim_loss_attempt_charge import _heartbeat_deps, _one_tick

    del wf_sql, flow, key, claim
    # Anchor the reconcile's grace to the PG clock (no wall-clock sleep):
    # the started stamp ages past one full lease (the deps' lease is 3 s;
    # the stamp is aged 6 s) so the claim-loss reconcile fires on the
    # first real tick.
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET started_at = '
            "clock_timestamp() - interval '6 seconds' WHERE id = $1",
            job,
        )
    deps = _heartbeat_deps(_dsn_by_schema[schema], schema, pool)
    await _one_tick(deps, worker)


async def _drive_reclaim_crashed(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    flow: JobId,
    job: JobId,
    key: str,
    worker: JobId,
    claim: Any,
) -> None:
    from taskq.workflows.api._attempt import AttemptClaim, repend_attempt

    del schema
    async with pool.acquire() as conn, conn.transaction():
        outcome = await repend_attempt(
            conn,
            wf_sql,
            claim=AttemptClaim(
                job_id=job,
                flow_id=flow,
                step_key=key,
                worker_id=worker,
                attempt=1,
                claim_epoch=2,
                ledger_id=claim.ledger_id,
                ledger_claimed_at=claim.claimed_at,
                map_index=None,
            ),
            error_class="InterfaceError",
            error_message="a transport blip escaped the finalize (the reclaim's own shape)",
            failures_before=0,
            max_attempts=1,
            retry_kind="transient",
            backoff_s=0.05,
            machinery_crashed=True,
        )
    assert outcome == "repended", outcome


@dataclass(frozen=True, slots=True)
class _Rep:
    driver: Any
    resting_status: str
    claim_status: str
    job_attempt: int
    reserved: bool
    cooperative_operator: bool


REPRESENTATIONS: dict[str, _Rep] = {
    "snooze": _Rep(_drive_snooze, "scheduled", "awaited", 0, False, False),
    "retry_after": _Rep(_drive_retry_after, "scheduled", "awaited", 0, False, False),
    "retry_after_non_consuming": _Rep(
        _drive_non_consuming, "scheduled", "awaited", 0, False, False
    ),
    "hold": _Rep(_drive_hold, "pending", "awaited", 1, False, False),
    "shutdown_drain": _Rep(_drive_shutdown_drain, "pending", "running", 0, True, False),
    "heartbeat_reconcile": _Rep(_drive_heartbeat_reconcile, "running", "running", 0, True, True),
    "reclaim_crashed": _Rep(_drive_reclaim_crashed, "scheduled", "crashed", 1, False, False),
}

#: The module's pg_dsn, keyed by schema name (the drain and the heartbeat
#: drivers build their own deps; the fixtures share the schema name).
_dsn_by_schema: dict[str, str] = {}


@pytest.fixture(autouse=True)
def _register_dsn(module_pg_schema: ModulePgSchema, wf_schema: str) -> None:
    _dsn_by_schema[wf_schema] = module_pg_schema.pg_dsn


async def _build_estate(
    wf_conn: asyncpg.Connection,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    schema: str,
    rep: str,
    spec: _Rep,
    *,
    seat: str,
) -> Estate:
    """One flow, the representation's target, its exact outer claim, and
    the full decoy estate. For the peer seat the target is additionally
    wired as a peer of a blocked fail-closed join with a failed parent."""
    extra_meta: dict[str, object] | None = None
    if rep == "retry_after_non_consuming":
        # the policy route's budget gate reads valid declared counter/limit
        # facts (int32-validated in the statement): 0 + 1 < 3 defers.
        extra_meta = {"wf_retry": {"non_consuming_count": 0, "non_consuming_limit": 3}}

    flow = await _seed_flow_root(wf_conn, schema)
    worker = JobId(_new_uuid_str())
    key = "target"
    job = await _seed_target(wf_conn, schema, flow, key, worker, extra_meta=extra_meta)
    claim = await _claim_outer(wf_pool, wf_sql, flow, job, key)
    assert claim.ledger_id is not None

    if seat == "peer":
        failed_parent = JobId(_new_uuid_str())
        await wf_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
            "max_attempts, retry_kind, step_key, error_class, finished_at, metadata, "
            "fairness_key, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'matrix', 'matrix', '{}', 'failed', 1, 1, 'transient', 'screen', "
            "'ValueError', clock_timestamp(), $2::jsonb, $3, 'workflow-run', $4)",
            failed_parent,
            _json.dumps({"flow_id": str(flow)}),
            str(flow),
            f"wf:{flow}:screen",
        )
        join = JobId(_new_uuid_str())
        await wf_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
            "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
            "created_at, fairness_key, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'matrix', 'matrix', '{}', 'pending', 3, 'transient', "
            "'join_step', 2, $2::jsonb, clock_timestamp() + interval '1 hour', "
            "clock_timestamp(), $3, 'workflow-run', $4)",
            join,
            _json.dumps({"flow_id": str(flow), "blocking_reason": "join"}),
            str(flow),
            f"wf:{flow}:join",
        )
        for parent in (failed_parent, job):
            await wf_conn.execute(
                f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, '
                "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
                join,
                parent,
                flow,
            )

    if spec.reserved:
        # the promoted source's page-width reservation: the drain and the
        # reconcile refund the claim WITHOUT touching metadata, so the
        # cancel's removal is exercised non-vacuously.
        await wf_conn.execute(
            f'UPDATE "{schema}".jobs SET metadata = metadata || $2::jsonb WHERE id = $1',
            job,
            _json.dumps({"wf_emit_reservation": {"version": 1, "width": 2}}),
        )

    await spec.driver(wf_pool, wf_sql, schema, flow, job, key, worker, claim)

    substep, iteration, older, terminal_history, map_sibling = await _seed_decoys(
        wf_conn, schema, flow, job, key, claim_status=spec.claim_status
    )

    # THE RESTING ESTATE (the representation's semantic disposition, before
    # any cancel): the row, the ordinal, the epoch, and the exact current
    # claim each read their documented resting shape.
    row = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, attempt::int AS attempt,
                   claim_epoch::bigint AS epoch, started_at
            FROM "{schema}".jobs WHERE id = $1""",
        job,
    )
    assert row is not None
    assert row["status"] == spec.resting_status, (rep, dict(row))
    assert int(row["attempt"]) == spec.job_attempt, (rep, dict(row))
    assert int(row["epoch"]) == 2, (rep, dict(row))
    if rep == "heartbeat_reconcile":
        # the reconcile's signature: the row stays Sweep 1's — running and
        # locked, the started stamp un-set.
        assert row["started_at"] is None, (rep, dict(row))
    outer_now = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, attempt::int AS attempt, error_class
            FROM "{schema}".wf_step_ledger WHERE id = $1""",
        claim.ledger_id,
    )
    assert outer_now is not None
    assert outer_now["status"] == spec.claim_status, (rep, dict(outer_now))
    assert int(outer_now["attempt"]) == 1, (rep, dict(outer_now))

    return Estate(
        flow=flow,
        job=job,
        worker=worker,
        outer=str(claim.ledger_id),
        substep=substep,
        iteration=iteration,
        older=older,
        terminal_history=terminal_history,
        map_sibling=map_sibling,
        key=key,
        resting_status=spec.resting_status,
        claim_status=spec.claim_status,
        job_attempt=spec.job_attempt,
        claim_attempt=1,
        reserved=spec.reserved,
        cooperative_operator=spec.cooperative_operator,
        decoys=(substep, iteration, older, terminal_history, map_sibling),
        decoy_names={
            substep: "ctx.substep",
            iteration: "loop.iter0",
            older: "older_epoch",
            terminal_history: "terminal_history",
            map_sibling: "map_sibling",
        },
    )


# ── the independent derived-capacity count ───────────────────────────────


async def _independent_capacity(
    conn: asyncpg.Connection, schema: str, flow: JobId
) -> tuple[int, int]:
    """The documented live-row/reservation predicate, counted directly —
    the product's own observation is asserted AGAINST this, never trusted
    alone. (These flows hold no exact parks, so the park exclusion is the
    blocking-reason marker test; the cell asserts the observation's own
    waiter absence separately.)"""
    row = await conn.fetchrow(
        f"""SELECT count(*) AS live_rows,
                   coalesce(sum(coalesce(
                       (metadata->'wf_emit_reservation'->>'width')::int, 0)), 0)
                   AS reservations
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1
              AND status IN ('pending', 'scheduled', 'running')
              AND metadata ? 'flow_id'
              AND step_key IS NOT NULL AND step_key <> '__flow__'
              AND NOT (metadata @> '{{"blocking_reason": "backpressure"}}'::jsonb
                       AND metadata ? 'wf_emit_backpressure')""",
        flow,
    )
    assert row is not None
    return int(row["live_rows"]), int(row["reservations"])


async def _assert_decoys_untouched(
    conn: asyncpg.Connection, schema: str, estate: Estate, before_stamps: dict[str, dict[str, Any]]
) -> None:
    rows = await conn.fetch(
        f"""SELECT id, status::text AS status, error_class, attempt::int AS attempt,
                   updated_at FROM "{schema}".wf_step_ledger
            WHERE id = ANY($1::uuid[])""",
        list(estate.decoys),
    )
    by_id = {str(r["id"]): dict(r) for r in rows}
    assert len(by_id) == 5, by_id
    for decoy_id, before in before_stamps.items():
        after = by_id[decoy_id]
        name = estate.decoy_names[decoy_id]
        assert after["status"] == before["status"], (
            f"the fence never rewrites a decoy claim ({name} / {decoy_id}): {before} -> {after}"
        )
        assert after["error_class"] == before["error_class"], (
            name,
            before,
            after,
        )
        assert after["attempt"] == before["attempt"], (name, before, after)
        assert str(after["updated_at"]) == str(before["updated_at"]), (
            name,
            before,
            after,
        )


# ── the matrix ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("seat", ["operator", "peer"])
@pytest.mark.parametrize("rep", sorted(REPRESENTATIONS))
async def test_cancel_of_every_refund_representation_is_claim_exact(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    rep: str,
    seat: str,
) -> None:
    """Seven representations x two seats: the exact current claim fences,
    the job terminalises, the reservation goes where held, ONE truthful
    event, the decoys untouched, no second disposition, and the derived
    capacity moves exactly once against an independent count. The
    heartbeat x operator cell is the cooperative arm (the row is running —
    no phase-1 terminal, no event, the reservation retained)."""
    from taskq.workflows._emit import observe_emit_capacity
    from taskq.workflows.api._sql_runner import (
        CANCEL_NODES_SQL_TEMPLATE,
        NODE_CLAIM_SQL_TEMPLATE,
        render_sql,
    )

    spec = REPRESENTATIONS[rep]
    estate = await _build_estate(wf_conn, wf_pool, wf_sql, wf_schema, rep, spec, seat=seat)

    # THE PRE-SEAT CENSUS: decoy stamps, the job's counters, the event
    # count, and the independent capacity.
    before_rows = await wf_conn.fetch(
        f"""SELECT id, status::text AS status, error_class, attempt::int AS attempt,
                   updated_at FROM "{wf_schema}".wf_step_ledger
            WHERE id = ANY($1::uuid[])""",
        list(estate.decoys),
    )
    before_stamps = {str(r["id"]): dict(r) for r in before_rows}
    assert len(before_stamps) == 5, before_stamps
    job_before = await wf_conn.fetchrow(
        f"""SELECT metadata, snooze_count::int AS snooze_count,
                   metadata->'wf_retry'->>'non_consuming_count' AS non_consuming_count
            FROM "{wf_schema}".jobs WHERE id = $1""",
        estate.job,
    )
    assert job_before is not None
    events_before = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1',
        estate.job,
    )
    independent_before = await _independent_capacity(wf_conn, wf_schema, estate.flow)
    observation_before = await observe_emit_capacity(_Conn(wf_pool), wf_sql, flow_id=estate.flow)
    assert (
        observation_before.live_rows,
        observation_before.reservations,
    ) == independent_before, (rep, seat, independent_before, observation_before)

    # THE SEAT.
    failed_parent: Any = None
    if seat == "operator":
        await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), estate.flow)
    else:
        from taskq.workflows.engine import _resolve_failed_parent, render_workflow_sql

        failed_parent = await wf_conn.fetchval(
            f"""SELECT id FROM "{wf_schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1
                  AND step_key = 'screen'""",
            estate.flow,
        )
        assert failed_parent is not None
        tx = wf_conn.transaction()
        await tx.start()
        try:
            await _resolve_failed_parent(
                wf_conn,
                render_workflow_sql(wf_schema),
                flow_id=estate.flow,
                parent_id=JobId(failed_parent),
                step_key="screen",
                map_index=None,
                error_class="ValueError",
                error_message="exhausted",
                error_traceback=None,
            )
            await tx.commit()
        except BaseException:
            await tx.rollback()
            raise

    if estate.cooperative_operator and seat == "operator":
        # THE COOPERATIVE ARM (the heartbeat x operator cell, by design):
        # phase 1 never terminalizes a running row — the row keeps its
        # claim, its reservation, and writes NO terminal event; the later
        # actual terminal writer records the running-to-terminal event.
        row = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class, metadata
                FROM "{wf_schema}".jobs WHERE id = $1""",
            estate.job,
        )
        assert row is not None
        assert row["status"] == "running", (rep, dict(row))
        meta = _detail(row["metadata"])
        assert meta.get("cancel_phase") == "cooperative", meta
        assert "wf_emit_reservation" in meta, meta
        events_after = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1',
            estate.job,
        )
        assert int(events_after) == int(events_before), (rep, events_after)
        outer_now = await wf_conn.fetchrow(
            f"""SELECT status::text AS status, error_class, updated_at
                FROM "{wf_schema}".wf_step_ledger WHERE id = $1""",
            estate.outer,
        )
        assert outer_now is not None
        assert outer_now["status"] == estate.claim_status, (rep, dict(outer_now))
        assert outer_now["error_class"] is None, dict(outer_now)
        await _assert_decoys_untouched(wf_conn, wf_schema, estate, before_stamps)
        independent_after = await _independent_capacity(wf_conn, wf_schema, estate.flow)
        assert independent_after == independent_before, (
            rep,
            independent_before,
            independent_after,
        )
        return

    # THE TERMINAL TRANSITION (every other cell).
    expected_class = "WorkflowCancelled" if seat == "operator" else "CancelledByPeerFailure"
    row = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class, finished_at, metadata,
                   attempt::int AS attempt, snooze_count::int AS snooze_count,
                   metadata->'wf_retry'->>'non_consuming_count' AS non_consuming_count
            FROM "{wf_schema}".jobs WHERE id = $1""",
        estate.job,
    )
    assert row is not None
    assert row["status"] == "cancelled", (rep, seat, dict(row))
    assert row["error_class"] == expected_class, (rep, seat, dict(row))
    assert row["finished_at"] is not None, (rep, seat, dict(row))
    # no second disposition: the ordinal the representation rested at is
    # the ordinal the cancel left (no refund from the wrong seat, no
    # ladder entry), the snooze counter never moved, the policy counter
    # never moved again, and the metadata delta is EXACTLY the terminal's
    # own (the reservation strip where held + the cooperative stamp).
    assert int(row["attempt"]) == estate.job_attempt, (rep, seat, dict(row))
    assert int(row["snooze_count"]) == int(job_before["snooze_count"]), (rep, seat)
    if rep == "retry_after_non_consuming":
        assert row["non_consuming_count"] == job_before["non_consuming_count"], (rep, seat)
    meta = _detail(row["metadata"])
    meta_before = _detail(job_before["metadata"])
    if seat == "operator":
        expected_meta = {k: v for k, v in meta_before.items() if k != "wf_emit_reservation"} | {
            "cancel_phase": "cooperative"
        }
    else:
        # the peer seat's structured cause rides the same terminal UPDATE
        # (the cancel-origin marker + the cascade pointer), never the
        # operator's cooperative stamp.
        expected_meta = {k: v for k, v in meta_before.items() if k != "wf_emit_reservation"} | {
            "peer_cancel": {
                "by": "peer_failure",
                "cascade_from": str(failed_parent),
            }
        }
    assert meta == expected_meta, (rep, seat, meta, expected_meta)

    # THE DECOYS FIRST: byte-for-byte untouched (status, class, ordinal,
    # instant) — a broken association's maximality probe lands its one
    # fence on a stamped-later decoy, and THIS is the red that names the
    # defect (the outer's own unfence is the second face of the same
    # defect, asserted next).
    await _assert_decoys_untouched(wf_conn, wf_schema, estate, before_stamps)

    # THE EXACT CURRENT-CLAIM FENCE.
    outer_now = await wf_conn.fetchrow(
        f"""SELECT status::text AS status, error_class FROM "{wf_schema}".wf_step_ledger
            WHERE id = $1""",
        estate.outer,
    )
    assert outer_now is not None
    assert outer_now["status"] == "fenced", (rep, seat, dict(outer_now))
    assert outer_now["error_class"] == expected_class, (rep, seat, dict(outer_now))

    # EXACTLY ONE TRUTHFUL EVENT.
    events = await wf_conn.fetch(
        f"""SELECT detail FROM "{wf_schema}".job_events
            WHERE job_id = $1 AND kind = 'state_change'""",
        estate.job,
    )
    assert len(events) == 1, (rep, seat, [dict(r) for r in events])
    detail = _detail(events[0]["detail"])
    assert detail["from_state"] == estate.resting_status, (rep, seat, detail)
    assert detail["to_state"] == "cancelled", detail
    assert detail["error_class"] == expected_class, detail
    assert int(detail["claim_attempt"]) == estate.claim_attempt, detail
    if seat == "peer":
        peer_cancel = detail.get("peer_cancel")
        assert isinstance(peer_cancel, dict), detail
        assert peer_cancel.get("by") == "peer_failure", detail
        assert peer_cancel["cascade_from"] == str(failed_parent), detail
    else:
        assert "peer_cancel" not in detail, detail
    if estate.reserved:
        assert int(detail["released_emit_reservation_width"]) == 2, detail
    else:
        assert "released_emit_reservation_width" not in detail, detail
    events_after_total = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".job_events WHERE job_id = $1', estate.job
    )
    assert int(events_after_total) - int(events_before) == 1, (
        rep,
        seat,
        events_after_total,
    )

    # THE ROW NEVER RE-ENTERS: the real claim statement answers nothing.
    claim_row = await wf_conn.fetchrow(
        render_sql(NODE_CLAIM_SQL_TEMPLATE, wf_schema),
        estate.job,
        JobId(_new_uuid_str()),
    )
    assert claim_row is None, (rep, seat)

    # THE DERIVED CAPACITY MOVED EXACTLY ONCE — against the independent count.
    independent_after = await _independent_capacity(wf_conn, wf_schema, estate.flow)
    expected_live_delta = -1
    expected_reservation_delta = -2 if estate.reserved else 0
    assert independent_before[0] - independent_after[0] == -expected_live_delta, (
        rep,
        seat,
        independent_before,
        independent_after,
    )
    assert independent_before[1] - independent_after[1] == -expected_reservation_delta, (
        rep,
        seat,
        independent_before,
        independent_after,
    )
    observation_after = await observe_emit_capacity(_Conn(wf_pool), wf_sql, flow_id=estate.flow)
    assert (
        observation_after.live_rows,
        observation_after.reservations,
    ) == independent_after, (rep, seat, independent_after, observation_after)
    # THE OBSERVATION'S OWN WAITER ABSENCE (the `_independent_capacity`
    # helper's documented claim, made true): these flows hold no exact
    # parks, so the resting observation reports NO waiter at all — a
    # spurious surviving park would surface here before any capacity
    # arithmetic could.
    assert observation_after.min_waiting_width is None, (rep, seat, observation_after)


# ── the strict single-row maximality inside the matrix (NEW-6) ───────────


async def test_duplicate_attempt_estate_inside_the_matrix_fences_exactly_one(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The maximality probe's own cell, on the matrix's snooze estate: a
    SECOND nonterminal row at the same (job, epoch, step, map) with a
    higher attempt (the ledger arbiter's distinct-row shape) must admit
    EXACTLY ONE row — the highest attempt — one fence, one event, the
    lower attempt untouched."""
    from taskq.workflows.api._sql_runner import CANCEL_NODES_SQL_TEMPLATE, render_sql

    estate = await _build_estate(
        wf_conn, wf_pool, wf_sql, wf_schema, "snooze", REPRESENTATIONS["snooze"], seat="operator"
    )

    duplicate = _new_uuid_str()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "map_index, attempt, claim_epoch, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, NULL, 3, 2, 'awaited', clock_timestamp(), "
        "clock_timestamp() + interval '10 microseconds')",
        duplicate,
        estate.flow,
        estate.job,
        estate.key,
    )

    await wf_conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, wf_schema), estate.flow)

    statuses = await wf_conn.fetch(
        f"""SELECT id, attempt::int AS attempt, status::text AS status, error_class
            FROM "{wf_schema}".wf_step_ledger WHERE id = ANY($1::uuid[])""",
        [estate.outer, duplicate],
    )
    by_attempt = {int(r["attempt"]): (r["status"], r["error_class"]) for r in statuses}
    assert len(statuses) == 2, statuses
    assert by_attempt[3] == ("fenced", "WorkflowCancelled"), by_attempt
    assert by_attempt[1][0] == "awaited", (
        f"exactly ONE row fences — the maximality probe admits the highest "
        f"attempt only: {by_attempt}"
    )
    events = await wf_conn.fetch(
        f"""SELECT detail FROM "{wf_schema}".job_events
            WHERE job_id = $1 AND kind = 'state_change'""",
        estate.job,
    )
    assert len(events) == 1, [dict(r) for r in events]
    detail = _detail(events[0]["detail"])
    assert int(detail["claim_attempt"]) == 3, detail
