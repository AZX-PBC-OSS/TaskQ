"""#717 OBLIGATION 1 — the identity boundary lane: a REAL worker SIGKILLed
between the commit emitting ordinal 32767 and the first statement emitting
ordinal 32768, then reclaimed, with the boundary's conservation asserted.

The issue's sentence, made executable for the shipped remedy A (one widened
space, all five carriers 32-bit → 64-bit): *"Kill the worker (real backend
termination) between the last record of partition N and the first statement
of N+1; on reclaim: zero records lost, zero duplicated, cursor handoff applied
exactly once, successor partition not double-created."*

THE DETERMINISTIC SEAM (no sleeping and hoping, no product test hook): the
source body emits page one — 32,768 records, ordinals ``0..32767``, so its
LAST row is the pre-widening ceiling — and then parks on the tier's own
release-gate handshake (``_wf_app.e2e_release_gate`` / ``_await_test_release``,
the same cross-process handshake the execution-policy controls use) BEFORE it
begins page two. Page one's transaction (children + edges + the cursor
checkpoint ``{"next": 32768}``) is therefore committed, and ordinal 32768 has
no statement, no row and no cursor. The test reads that durable state, SIGKILLs
the pod whose ``workers.id`` holds the source claim, and opens the gate for the
reclaim.

WHAT IS ASSERTED, and why each leg can FAIL:

* the pre-kill window is read, not assumed: 32,768 record rows, ordinals
  ``0..32767`` exactly, the cursor checkpoint at 32768, and NO ordinal-32768 row;
* the lease disposition is asserted rather than assumed: immediately after the
  SIGKILL the source row is still ``running`` under the corpse's worker with a
  lease that had NOT lapsed (nothing but the expiry may take that row away),
  and the corpse's registered heartbeat froze — or its worker row was reaped as
  stale. Both are honest dispositions and each is named in the failure;
* the reclaim is applied EXACTLY ONCE: the source's physical attempt advances
  ``1 -> 2`` and never further, and the crashed attempt's ledger row is the
  phantom the terminal sweep fences (never a row left ``running`` forever,
  never a terminal lie);
* the cursor handoff is the CHECKPOINT's: the successor row (ordinal 32768)
  was created AFTER the kill by the reclaimed attempt, at ordinal 32768 — never
  recomputed, never modulo;
* zero lost / zero duplicated: the record set is exactly ``0..32768``, one row
  per ordinal, with distinct trace ids and idempotency keys, and one SUCCEEDED
  ledger terminal per identity. The reclaim re-executes EVERY record row the
  corpse held in flight when it died — the boundary's zero-loss direction,
  asserted exactly. A dispatcher claims rows beside the body it runs, so a
  pre-kill claim gate (a bounded ``_wait_for``) steers the SIGKILL onto a
  window where the victim provably holds at least one LEDGERED record claim —
  the set is snapshotted at the kill as one ``(id, attempt)`` mapping and
  asserted non-empty. Attempt > 1 is NOT treated as crash-only: the
  machinery's own at-least-once reclaim (a lapsed lease handed back
  mid-flight, a heartbeat-lost isolation, a repend with backoff) can
  re-execute a row the corpse never held, so the re-executed set is bounded
  (one re-execution per row) and terminal-deduped (one succeeded terminal per
  identity), and every re-executed row's abandoned ledger claim must fence.
  The fenced phantoms: every corpse ledger claim fenced (zero lost phantoms),
  no corpse row fenced beyond its own ledger claims (a claimed-but-
  unledgered row has no phantom to fence), and every fenced row one the run
  re-executed (a fence on a never-re-executed row is a lost record wearing a
  fence);
* the successor is not double-created: exactly one row (live or archived) for
  ordinal 32768, exactly one succeeded ledger row, no join-delivery outbox row,
  no hold row;
* the boundary PAIR (ordinals 32767 and 32768) reconciles across
  ``jobs``/``jobs_archive``/``wf_step_ledger``/``wf_outbox``/``wf_signals``,
  and the run's tagged population — re-stamped and coverage-asserted (tagged
  count == run-row count) immediately before the read, per
  ``tag_run_rows``' census contract — closes the tier's shared invariants.

THE RODE INFRASTRUCTURE (named, per the obligation): ``spawn_wf_fleet`` /
``_wf_entry``'s real subprocess pods with the march registry (``_wf_app``);
``_wf_app``'s release-gate handshake for the deterministic pause; the
worker-deploy cell's lease/reclaim path (``test_wf_deploy_matrix.py`` cell 1 —
SIGKILL the holder, the lease/reclaim machinery re-pends, a surviving pod
finishes) plus the orchestrator's own restart (a replacement real PID is
spawned after the crash); ``_wf_harness``' readers and ``RECLAIM_BOUND_S``
(RECLAIM_BOUND_S = (lease + sweep + 5s + the long node) x stretch); the tier's
shared invariants.

TWO SCENARIO KNOBS, both declared and neither a weakening:

* the boundary flow declares ``max_in_flight=None`` — a 32,768-record page can
  fit no bounded default by construction, so the expiring page width is the
  boundary itself;
* the SOURCE's execution policy outranks its records (priority 10 vs the
  records' default 0), so the reclaim is dispatched AHEAD of the emitted
  backlog. The FIFO alternative would make the reclaim bound a function of
  32,768 queued rows rather than of the lease; the page split, the record count
  and the kill window are untouched.

RED-FIRST: the lane is proved able to fail by a scratch mutant of the durable
checkpoint's READ half (``StepContext.cursor`` returning ``{}`` — the resume
that ignores the committed checkpoint). Page one still commits and the kill
window still stages, so the kill is NOT what the mutant breaks: the reclaimed
attempt re-reads an empty cursor, re-pages from ordinal 0, and the emit's
idempotency arbiter kills the source with ``PageDivergedError`` — this file's
reclaim window reds (measured: failed at 102s, source 'failed', attempt 2,
cursor stuck at 32768). Recorded in the work record.
"""

# ruff: noqa: S608  # Why: every query's schema identifier is the fixture-validated module schema and every value is $-bound; only the f-string interpolation of the schema is flagged.

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
import subprocess
import time
from datetime import datetime
from typing import Any

import asyncpg
import pytest

from taskq.workflows import FlowRunner
from taskq.workflows._fork import FORK_CHUNK
from tests.system_e2e._harness import (
    SWEEP_INTERVAL_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
)
from tests.system_e2e._invariants import assert_balanced
from tests.system_e2e._wf_app import (
    BOUNDARY_FIRST_WIDE_ORDINAL,
    BOUNDARY_GATE,
    BOUNDARY_LAST_NARROW_ORDINAL,
    BOUNDARY_RECORDS,
    BOUNDARY_STEP,
    MARCH_FLOWS,
    clear_e2e_release_gate,
    e2e_release_gate_ddl,
    open_e2e_release_gate,
    seed_e2e_release_gate,
)
from tests.system_e2e._wf_harness import (
    MARCH_LOCK_LEASE_S,
    RECLAIM_BOUND_S,
    spawn_wf_fleet,
    tag_run_rows,
)

pytestmark = [
    pytest.mark.system,
    pytest.mark.integration,
    pytest.mark.load_sensitive,
]

#: The tag this file's population carries (the tier's invariant convention).
_TAG = "wf-identity-boundary"

#: Page one is one transaction of ``32768 / FORK_CHUNK`` children INSERTs plus
#: the same count of edge INSERTs plus the cursor checkpoint. The per-chunk
#: budget is a chunk's two round trips + the tx's WAL with a load margin
#: (MEASURED here: 66 chunks commit in ~0.09s); the bound is that count x the
#: budget x the tier's co-tenancy stretch. The MEASURED value is printed
#: beside it.
_PAGE_ONE_CHUNKS = BOUNDARY_FIRST_WIDE_ORDINAL // FORK_CHUNK + 1
_EMIT_CHUNK_BUDGET_S = 0.25
_PAGE_ONE_BOUND_S = _PAGE_ONE_CHUNKS * _EMIT_CHUNK_BUDGET_S * TIER_LOAD_STRETCH

#: The record settle bound: the lane executes one trivial body per record
#: (32,769) on two real worker pods once the crashed replica is replaced.
#: PRICE = this lane's own measured per-record price (a full green run took
#: 494.51s end to end, of which ~450s is the record settle: 13.7 ms/record)
#: x the tier's co-tenancy stretch, plus the reclaim bound (the source's crash
#: recovery precedes the children's settle). Measured beside the bound, never
#: a bare constant.
_RECORD_PRICE_S = 0.0137 * TIER_LOAD_STRETCH
_RECORD_SETTLE_BOUND_S = RECLAIM_BOUND_S + BOUNDARY_RECORDS * _RECORD_PRICE_S

#: The phantom fence's bound: the sweep arms run per ``SWEEP_INTERVAL_S``; the
#: fenced-attempt reap fires once the flow is terminal. Stretched twice for the
#: sweep's own batching.
_PHANTOM_FENCE_BOUND_S = SWEEP_INTERVAL_S * 10.0 * TIER_LOAD_STRETCH

#: The pre-kill claim gate's bound, in the lane's own shape: a record's
#: stretched price (``_RECORD_PRICE_S``) is the per-row settle cost, and one
#: dispatch cycle is a poll interval (``TASKQ_POLL_INTERVAL``, 50 ms in this
#: fleet) that claims up to the victim's four slots — so 128 record prices is
#: dozens of rotations of those slots, far above the one cycle the gate needs
#: and far below the page budget. Measured beside it at the gate; never a bare
#: constant.
_CLAIM_GATE_BOUND_S = _RECORD_PRICE_S * 128.0

_DB_CLOCK_SQL = "SELECT clock_timestamp()"


def _cursor_of(document: object) -> dict[str, object]:
    decoded: Any = json.loads(document) if isinstance(document, str) else document
    assert isinstance(decoded, dict), f"the cursor checkpoint is not a jsonb object: {decoded!r}"
    return decoded


async def _source_row(conn: asyncpg.Connection, schema: str, source_id: Any) -> dict[str, Any]:
    row = await conn.fetchrow(
        f"""SELECT status::text AS status, attempt, claim_epoch,
                   locked_by_worker::text AS holder, lock_expires_at,
                   metadata->'emit_cursor' AS cursor, created_at
            FROM "{schema}".jobs WHERE id = $1""",
        source_id,
    )
    assert row is not None, f"the source row {source_id} vanished"
    return dict(row)


async def _record_facts(conn: asyncpg.Connection, schema: str, source_id: Any) -> dict[str, Any]:
    row = await conn.fetchrow(
        f"""SELECT count(*)::bigint AS rows,
                   count(DISTINCT map_index)::bigint AS identities,
                   count(DISTINCT trace_id)::bigint AS traces,
                   count(DISTINCT idempotency_key)::bigint AS keys,
                   min(map_index) AS minimum,
                   max(map_index) AS maximum,
                   count(*) FILTER (WHERE map_index = $2::bigint) AS successor_rows,
                   count(*) FILTER (WHERE map_index = $3::bigint) AS boundary_rows,
                   count(*) FILTER (WHERE map_index > $3::bigint) AS above_boundary,
                   max(created_at) FILTER (WHERE map_index <= $3::bigint) AS last_narrow_at,
                   count(*) FILTER (WHERE status IN ('succeeded', 'failed', 'cancelled'))
                       AS terminal_rows,
                   count(*) FILTER (WHERE finished_at IS NULL) AS unfinished
            FROM "{schema}".jobs
            WHERE parent_id = $1 AND step_key = $4""",
        source_id,
        BOUNDARY_FIRST_WIDE_ORDINAL,
        BOUNDARY_LAST_NARROW_ORDINAL,
        BOUNDARY_STEP,
    )
    assert row is not None
    return dict(row)


async def _dump_boundary(
    conn: asyncpg.Connection, schema: str, flow_id: Any, source_id: Any
) -> None:
    """A failed window names its own rows (the marches' diagnostic discipline):
    the source's durable state, its record census, its event trail and the
    registered workers — the read a debrief needs, printed before the red."""
    source = await conn.fetchrow(
        f"""SELECT status::text AS status, attempt, claim_epoch,
                   locked_by_worker::text AS holder, lock_expires_at,
                   metadata->'emit_cursor' AS cursor, error_class
            FROM "{schema}".jobs WHERE id = $1""",
        source_id,
    )
    events = await conn.fetch(
        f"""SELECT kind, detail FROM "{schema}".job_events
            WHERE job_id = $1 ORDER BY id DESC LIMIT 8""",
        source_id,
    )
    counts = await conn.fetchrow(
        f"""SELECT count(*)::int AS rows,
                   coalesce(min(map_index), -1) AS minimum,
                   coalesce(max(map_index), -1) AS maximum
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    workers = await conn.fetch(
        f'SELECT id::text, pid, last_seen_at FROM "{schema}".workers ORDER BY pid'
    )
    print(f"[boundary] EXPIRED WINDOW: source={dict(source) if source else None}")
    print(f"[boundary] EXPIRED WINDOW: rows={dict(counts) if counts else None}")
    print(f"[boundary] EXPIRED WINDOW: events={[dict(r) for r in events]}")
    print(f"[boundary] EXPIRED WINDOW: workers={[dict(r) for r in workers]}")


async def _wait_for(
    predicate: Any,
    *,
    interval_s: float,
    deadline_s: float,
    what: str,
    on_expiry: Any = None,
) -> float:
    """Poll *predicate* to truth, returning the MEASURED wait. The bound is
    the caller's derived value; the failure names the wait and its bound, and
    a supplied ``on_expiry`` prints the rows the expired wait was reading."""
    start = time.monotonic()
    while True:
        if await predicate():
            return time.monotonic() - start
        measured = time.monotonic() - start
        if measured >= deadline_s:
            if on_expiry is not None:
                await on_expiry()
            raise AssertionError(
                f"the wait expired after {measured:.2f}s (bound {deadline_s:.2f}s): {what}"
            )
        await asyncio.sleep(interval_s)


async def _report_noncomplete(
    conn: asyncpg.Connection, schema: str, flow_id: str, status: str
) -> None:
    """A non-complete terminal verdict names its own rows: the census, every
    non-succeeded node with its error/blocking fields, the source and the
    root — the read a debrief needs, printed before the red."""
    census = await conn.fetch(
        f"""SELECT step_key, status::text AS status, count(*)::int AS rows
            FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid
            GROUP BY step_key, status ORDER BY step_key, status""",
        flow_id,
    )
    bad = await conn.fetch(
        f"""SELECT step_key, coalesce(map_index::text, '-') AS mi, status::text AS status,
                   attempt, error_class, left(coalesce(error_message, ''), 300) AS message,
                   coalesce(metadata->>'blocking_reason', '-') AS blocking
            FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND status NOT IN ('succeeded')
            ORDER BY step_key, map_index LIMIT 25""",
        flow_id,
    )
    root = await conn.fetchrow(
        f'SELECT status::text AS status, attempt FROM "{schema}".jobs WHERE id = $1',
        flow_id,
    )
    print(f"[boundary] NON-COMPLETE {status}: census={[dict(r) for r in census]}")
    print(f"[boundary] NON-COMPLETE root={dict(root) if root else None}")
    print(f"[boundary] NON-COMPLETE rows={[dict(r) for r in bad]}")


async def _wait_terminal(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float = _RECORD_SETTLE_BOUND_S
) -> tuple[str, float]:
    """Wait for the derived terminal verdict, printing the record census as it
    climbs (the MEASURED value beside the derived bound — the harness's own
    contract). The verdict is the rows-only reconstruction, never a cache."""
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._status import reconstruct_workflow_status

    wsql = WorkflowSql.build(schema)
    start = time.monotonic()
    next_note = 0.0
    while True:
        elapsed = time.monotonic() - start
        status = await reconstruct_workflow_status(conn, wsql, JobId(flow_id))
        if status in ("complete", "failed", "cancelled"):
            if status != "complete":
                await _report_noncomplete(conn, schema, flow_id, status)
            return (status, elapsed)
        if elapsed >= next_note:
            row = await conn.fetchrow(
                f"""SELECT count(*) FILTER (WHERE step_key = $2)::int AS records,
                           count(*) FILTER (WHERE step_key = $2
                                             AND status IN ('succeeded', 'failed', 'cancelled'))
                               ::int AS terminal
                    FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
                flow_id,
                BOUNDARY_STEP,
            )
            assert row is not None
            print(
                f"[boundary] settle: elapsed={elapsed:.0f}s bound={bound_s:.0f}s "
                f"records={row['records']} terminal={row['terminal']} status={status}"
            )
            next_note = elapsed + 20.0
        assert elapsed < bound_s, (
            f"the boundary run did not terminalize within {bound_s:.2f}s "
            f"(derived bound; measured {elapsed:.2f}s, status {status!r})"
        )
        await asyncio.sleep(0.5)


async def _stop_fleet(fleet: dict[str, WorkerProc]) -> None:
    for pod in fleet.values():
        if pod.proc.poll() is None:
            pod.proc.send_signal(signal.SIGTERM)
            with contextlib.suppress(subprocess.TimeoutExpired):
                pod.proc.wait(timeout=5)
        reap(pod)
    fleet.clear()


@pytest.mark.timeout(1800)
async def test_sigkill_between_ordinal_32767_and_32768_reclaims_with_cursor_handoff(
    pg_dsn: str,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    conn = await asyncpg.connect(pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        # ── PRE-CONDITION: the shipped remedy A's schema (all five carriers
        # widened). On the pre-widening schema ordinal 32768 is illegal and
        # this lane's boundary cannot even be staged.
        carriers = await conn.fetch(
            """SELECT table_name, data_type FROM information_schema.columns
               WHERE table_schema = $1 AND column_name = 'map_index'
               ORDER BY table_name""",
            schema,
        )
        types = {str(r["table_name"]): str(r["data_type"]) for r in carriers}
        assert set(types) >= {
            "jobs",
            "jobs_archive",
            "wf_step_ledger",
            "wf_signals",
            "wf_outbox",
        }, f"the identity carriers are missing from this schema: {types}"
        assert set(types.values()) == {"bigint"}, (
            f"the boundary lane requires the fully widened schema (remedy A); got {types}"
        )

        # ── ARRANGE: the release gate (closed), then the run.
        await conn.execute(e2e_release_gate_ddl(schema))
        await seed_e2e_release_gate(conn, schema, BOUNDARY_GATE)
        runner = FlowRunner(MARCH_FLOWS["identity_boundary_kill"], pool, schema)  # pyright: ignore[reportArgumentType]  # Why: the compiled graph's module identity is the march app's; the runner accepts the CompiledWorkflow face.
        flow_id = (await runner.create_flow()).flow_id
        await tag_run_rows(pool, schema, flow_id, _TAG)
        source_id = await conn.fetchval(
            f"""SELECT id FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
            flow_id,
        )
        assert source_id is not None, "the flow has no source node"
        print(f"[boundary] flow={flow_id} source={source_id}")

        fleet = await spawn_wf_fleet(conn, pg_dsn, schema, ["w1", "w2"])

        # ── THE WINDOW: page one committed (ordinals 0..32767 + the cursor
        # checkpoint at 32768), the successor page not begun.
        async def _page_one_committed() -> bool:
            row = await _source_row(conn, schema, source_id)
            if row["status"] != "running" or row["holder"] is None:
                return False
            facts = await _record_facts(conn, schema, source_id)
            return (
                _cursor_of(row["cursor"]) == {"next": BOUNDARY_FIRST_WIDE_ORDINAL}
                and int(facts["rows"]) == BOUNDARY_FIRST_WIDE_ORDINAL
                and facts["minimum"] == 0
                and facts["maximum"] == BOUNDARY_LAST_NARROW_ORDINAL
                and int(facts["successor_rows"]) == 0
            )

        measured = await _wait_for(
            _page_one_committed,
            interval_s=0.25,
            deadline_s=_PAGE_ONE_BOUND_S,
            what="page one's commit (ordinals 0..32767 + the cursor checkpoint at 32768)",
        )
        print(f"[boundary] page-one: measured={measured:.2f}s bound={_PAGE_ONE_BOUND_S:.2f}s")
        window = await _source_row(conn, schema, source_id)
        facts = await _record_facts(conn, schema, source_id)
        assert set(facts) and int(facts["rows"]) == BOUNDARY_FIRST_WIDE_ORDINAL, facts
        assert int(facts["identities"]) == BOUNDARY_FIRST_WIDE_ORDINAL, facts
        assert int(facts["successor_rows"]) == 0, (
            "ordinal 32768 existed BEFORE the kill — the page split is not at the boundary"
        )
        assert window["attempt"] == 1, window
        victim_worker = window["holder"]
        assert victim_worker is not None, window

        # ── THE KILL: the REAL OS process that holds the source claim.
        victim_pid = await conn.fetchval(
            f'SELECT pid FROM "{schema}".workers WHERE id = $1::uuid', victim_worker
        )
        assert victim_pid is not None, (
            f"the source's holder {victim_worker} has no registered worker row "
            f"(pids: {[(n, p.proc.pid) for n, p in fleet.items()]})"
        )
        victim = next((n for n, p in fleet.items() if p.proc.pid == victim_pid), None)
        assert victim is not None, (
            f"the source's holder pid {victim_pid} is not a fleet pod "
            f"(pids: {[(n, p.proc.pid) for n, p in fleet.items()]})"
        )

        # ── THE CLAIM GATE: wait (bounded, derived) until the victim
        # provably holds a LEDGERED record claim, so the SIGKILL interrupts a
        # non-empty claim set and the retry/fence legs run with n > 0 instead
        # of passing vacuously on ∅ == ∅. The predicate is an aggregate EXISTS
        # over the claim join — never a second id-set snapshot — and minimal
        # work (a liveness check + one heartbeat read) sits between gate-true
        # and the signal, so the victim cannot rotate the claim away first.
        async def _victim_holds_a_ledgered_record() -> bool:
            return bool(
                await conn.fetchval(
                    f"""SELECT EXISTS (
                            SELECT 1 FROM "{schema}".jobs j
                                     JOIN "{schema}".wf_step_ledger l
                                       ON l.flow_id = $1::uuid AND l.step_key = $2
                                      AND l.job_id = j.id AND l.attempt = j.attempt
                            WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2
                              AND j.status = 'running' AND l.status = 'running')""",
                    flow_id,
                    BOUNDARY_STEP,
                    victim_worker,
                )
            )

        async def _dump_victim_claims() -> None:
            await _dump_boundary(conn, schema, flow_id, source_id)
            rows = await conn.fetch(
                f"""SELECT j.id::text, j.attempt, (l.job_id IS NOT NULL) AS ledgered
                    FROM "{schema}".jobs j
                             LEFT JOIN "{schema}".wf_step_ledger l
                                    ON l.flow_id = $1::uuid AND l.step_key = $2
                                   AND l.job_id = j.id AND l.attempt = j.attempt
                    WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2
                      AND j.status = 'running'""",
                flow_id,
                BOUNDARY_STEP,
                victim_worker,
            )
            print(
                f"[boundary] CLAIM-GATE EXPIRED: victim {victim_worker} "
                f"running claims={[dict(r) for r in rows]}"
            )

        claim_wait = await _wait_for(
            _victim_holds_a_ledgered_record,
            interval_s=0.02,
            deadline_s=_CLAIM_GATE_BOUND_S,
            what="the victim to hold a ledgered in-flight record claim",
            on_expiry=_dump_victim_claims,
        )
        print(f"[boundary] claim-gate: measured={claim_wait:.2f}s bound={_CLAIM_GATE_BOUND_S:.2f}s")

        # ── THE KILL: the REAL OS process that holds the source claim,
        # witnessed: alive immediately before the signal, dead BY the signal
        # after it (reap() would kill a survivor itself, so only the liveness
        # half attributes the death to the lane's SIGKILL).
        victim_proc = fleet[victim].proc
        assert victim_proc.poll() is None, (
            f"the victim {victim} (pid {victim_pid}) is not alive before the SIGKILL"
        )
        heartbeat_before = await conn.fetchval(
            f'SELECT last_seen_at FROM "{schema}".workers WHERE id = $1::uuid', victim_worker
        )
        fleet[victim].proc.send_signal(signal.SIGKILL)
        reap(fleet[victim])
        del fleet[victim]
        killed_at = await conn.fetchval(_DB_CLOCK_SQL)
        assert isinstance(killed_at, datetime)
        assert victim_proc.returncode == -9, (
            f"the SIGKILLed worker exited with {victim_proc.returncode}, not -9 "
            f"— the death was not the lane's own signal"
        )
        print(
            f"[boundary] SIGKILL witnessed -> {victim} (pid {victim_pid}): "
            f"alive before, returncode {victim_proc.returncode}, at {killed_at.isoformat()}"
        )

        # ── THE CORPSE'S IN-FLIGHT RECORD CLAIMS, snapshotted now as ONE
        # (job id -> crashed attempt) mapping: a worker's dispatcher claims
        # rows beside the body it runs, so the SIGKILL can interrupt record
        # attempts too. Whatever this mapping is, the reclaim must re-execute
        # EVERY one of its ids (the zero-loss leg, asserted at the end); extra
        # re-executions belong to the machinery's at-least-once path and are
        # bounded below. The fence leg keys on the SAME crashed attempt —
        # never post-reclaim ``jobs.attempt``, which the reclaim advances to 2.
        corpse_records = {
            str(r["id"]): int(r["attempt"])
            for r in await conn.fetch(
                f"""SELECT id, attempt FROM "{schema}".jobs
                    WHERE locked_by_worker = $1::uuid AND step_key = $2
                      AND status = 'running'""",
                victim_worker,
                BOUNDARY_STEP,
            )
        }
        print(f"[boundary] corpse in-flight record claims: {len(corpse_records)}")
        # Fail-closed: the claim gate forced a non-empty set at the kill; if
        # the victim rotated into an all-unledgered batch anyway, red here —
        # never let the set legs compare ∅ == ∅.
        assert len(corpse_records) > 0, (
            "the corpse held no in-flight record claim at the SIGKILL — the "
            "retry and fence legs would be vacuous; the pre-kill claim gate "
            "should have forced n > 0"
        )

        # ── THE CORPSE'S LEDGER-CLAIMED SUBSET (fail-closed, read before the
        # ~500s settle): which snapshotted (job_id, attempt) pairs already
        # have a wf_step_ledger row — status-UNFILTERED (a status predicate
        # would make the fence leg a tautology) and derived from the ONE
        # snapshot, never joined to post-reclaim jobs.attempt. Only these
        # rows are phantoms PHANTOM_REAP_SQL can fence; a claimed-but-
        # unledgered row has no phantom and must be allowed none.
        corpse_ledger_claims = {
            (str(r["job_id"]), int(r["attempt"]))
            for r in await conn.fetch(
                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1 AND step_key = $2""",
                flow_id,
                BOUNDARY_STEP,
            )
        } & {(job_id, corpse_records[job_id]) for job_id in corpse_records}
        print(
            f"[boundary] corpse ledger-claimed subset: {len(corpse_ledger_claims)} "
            f"of {len(corpse_records)} record claims"
        )
        assert len(corpse_ledger_claims) > 0, (
            "the corpse's ledger-claimed subset is empty at the kill — the "
            "fence leg would compare ∅ == ∅ (corpse claims: "
            f"{sorted(corpse_records)})"
        )

        # ── THE LEASE DISPOSITION, asserted immediately (before any reclaim):
        # the corpse still owns a running row whose lease had NOT lapsed —
        # nothing but the expiry may take that row away.
        corpse = await _source_row(conn, schema, source_id)
        assert corpse["status"] == "running", (
            f"the source left 'running' before its lease could lapse: {corpse}"
        )
        assert corpse["holder"] == victim_worker, (
            f"the source's holder moved before the lease lapsed: {corpse}"
        )
        assert corpse["attempt"] == 1, f"the attempt advanced before the reclaim: {corpse}"
        assert corpse["lock_expires_at"] is not None, corpse
        # DB-clock-to-DB-clock: the lease's expiry and the kill instant both
        # come from the database (clock_timestamp()), so no host clock — and
        # no host/DB skew — can enter this disposition.
        remaining_s = (corpse["lock_expires_at"] - killed_at).total_seconds()
        assert 0.0 < remaining_s <= MARCH_LOCK_LEASE_S, (
            f"the corpse's lease is not the fleet's own lease window "
            f"(remaining {remaining_s:.2f}s, lease {MARCH_LOCK_LEASE_S}s): {corpse}"
        )
        print(
            f"[boundary] lease: remaining={remaining_s:.2f}s of {MARCH_LOCK_LEASE_S}s "
            f"(DB clock only)"
        )

        # ── THE RESTART: the orchestrator's own recovery — the crashed
        # replica is replaced by a fresh real PID. The reclaim may land on
        # either surviving pod; the boundary property does not depend on which.
        fleet.update(await spawn_wf_fleet(conn, pg_dsn, schema, ["w1b"], log_sink=None))
        print(f"[boundary] restarted -> w1b (pids {sorted(p.proc.pid for p in fleet.values())})")

        # ── THE RELEASE: the gate opens only after the observation above, so
        # the reclaimed attempt is the one that emits the successor page.
        await open_e2e_release_gate(conn, schema, BOUNDARY_GATE)

        # ── THE RECLAIM: the source's own row is re-pended and re-claimed
        # exactly once; its successor page commits.
        async def _successor_committed() -> bool:
            row = await _source_row(conn, schema, source_id)
            if row["attempt"] != 2 or row["status"] != "succeeded":
                return False
            facts = await _record_facts(conn, schema, source_id)
            return (
                _cursor_of(row["cursor"]) == {"next": BOUNDARY_RECORDS}
                and int(facts["rows"]) == BOUNDARY_RECORDS
                and int(facts["successor_rows"]) == 1
            )

        async def _dump_reclaim() -> None:
            await _dump_boundary(conn, schema, flow_id, source_id)

        measured = await _wait_for(
            _successor_committed,
            interval_s=0.25,
            deadline_s=RECLAIM_BOUND_S,
            what="the reclaim's successor-page commit (attempt 2, cursor 32769)",
            on_expiry=_dump_reclaim,
        )
        print(f"[boundary] reclaim: measured={measured:.2f}s bound={RECLAIM_BOUND_S:.2f}s")

        # ── the reclaimed source's boundary record.
        reclaimed = await _source_row(conn, schema, source_id)
        assert reclaimed["attempt"] == 2, (
            f"the reclaim was not applied exactly once (attempt {reclaimed['attempt']}): {reclaimed}"
        )
        assert reclaimed["holder"] != victim_worker, (
            f"the corpse still holds the source after its reclaim: {reclaimed}"
        )
        assert _cursor_of(reclaimed["cursor"]) == {"next": BOUNDARY_RECORDS}, reclaimed

        facts = await _record_facts(conn, schema, source_id)
        assert int(facts["rows"]) == BOUNDARY_RECORDS, facts
        assert int(facts["successor_rows"]) == 1, (
            f"the successor ordinal was created more than once: {facts}"
        )
        assert int(facts["above_boundary"]) == 1, facts
        assert facts["last_narrow_at"] is not None and facts["last_narrow_at"] < killed_at, (
            f"page one's last row was not durable BEFORE the kill: last_narrow_at="
            f"{facts['last_narrow_at']} killed_at={killed_at}"
        )
        successor = await conn.fetchrow(
            f"""SELECT id, created_at, attempt FROM "{schema}".jobs
                WHERE parent_id = $1 AND step_key = $2 AND map_index = $3::bigint""",
            source_id,
            BOUNDARY_STEP,
            BOUNDARY_FIRST_WIDE_ORDINAL,
        )
        assert successor is not None
        assert successor["created_at"] > killed_at, (
            f"ordinal 32768 predates the kill — the page split is not the boundary: "
            f"created_at={successor['created_at']} killed_at={killed_at}"
        )

        # ── the corpse's heartbeat froze: no process renewed it after the
        # SIGKILL. The stale reaper may have removed the row entirely — both
        # are honest dispositions, and each is named.
        heartbeat_after = await conn.fetchval(
            f'SELECT last_seen_at FROM "{schema}".workers WHERE id = $1::uuid', victim_worker
        )
        assert heartbeat_after is None or heartbeat_after == heartbeat_before, (
            f"the SIGKILLed worker's heartbeat advanced after death "
            f"(before={heartbeat_before}, after={heartbeat_after}) — a dead process renewed its lease"
        )

        # ── the run terminalizes from its rows (all 32,769 records execute).
        status, measured = await _wait_terminal(conn, schema, str(flow_id))
        print(f"[boundary] terminal: measured={measured:.2f}s bound={_RECORD_SETTLE_BOUND_S:.2f}s")
        if status != "complete":
            rows = await conn.fetch(
                f"""SELECT step_key, coalesce(map_index::text,'-') AS mi,
                           status::text AS status, attempt, error_class
                    FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid
                    ORDER BY step_key, map_index LIMIT 20""",
                flow_id,
            )
            print(f"[boundary] FAILED RUN'S ROWS: {[dict(r) for r in rows]}")
        assert status == "complete", f"the boundary run derived {status!r}, not complete"

        # ── ZERO LOST, ZERO DUPLICATED, across the boundary.
        final = await _record_facts(conn, schema, source_id)
        assert int(final["rows"]) == BOUNDARY_RECORDS, final
        assert int(final["identities"]) == BOUNDARY_RECORDS, final
        assert int(final["traces"]) == BOUNDARY_RECORDS, final
        assert int(final["keys"]) == BOUNDARY_RECORDS, final
        assert final["minimum"] == 0 and final["maximum"] == BOUNDARY_FIRST_WIDE_ORDINAL, final
        assert int(final["terminal_rows"]) == BOUNDARY_RECORDS, final
        assert int(final["unfinished"]) == 0, final
        # ── THE RETRY LEG: every record the corpse held in flight is
        # re-executed (zero lost), every re-execution is bounded, and every
        # terminal is deduped — the three directions the reclaim machinery
        # actually owns (below).
        retried = {
            str(r["id"])
            for r in await conn.fetch(
                f"""SELECT id FROM "{schema}".jobs
                    WHERE parent_id = $1 AND step_key = $2 AND attempt > 1""",
                source_id,
                BOUNDARY_STEP,
            )
        }
        # The reclaim re-executed EVERY record the corpse held in flight —
        # the boundary's ZERO-LOSS direction, exact.
        lost = set(corpse_records) - retried
        assert not lost, (
            f"the reclaim lost {len(lost)} of the corpse's in-flight record "
            f"claims (never re-executed): {sorted(lost)}"
        )
        # A re-execution beyond the corpse's claims is the machinery's own
        # at-least-once reclaim (a lapsed lease handed back mid-flight, a
        # heartbeat-lost isolation, a repend with backoff) — the mechanism
        # does not promise attempt > 1 means corpse-held (measured: one
        # non-corpse re-claim in a 450s settle, zero lost, one terminal per
        # identity). It is BOUNDED: each row re-executes at most once more
        # (beyond_reclaim below), its terminal is deduped by the ledger
        # (the exactly-one-succeeded legs below), and its abandoned claim
        # must fence beside the corpse's (the fence leg).
        print(
            f"[boundary] re-executed: {len(retried)} rows — corpse claims "
            f"{len(corpse_records)}, machinery re-claims "
            f"{len(retried - set(corpse_records))}"
        )
        beyond_reclaim = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE parent_id = $1 AND step_key = $2 AND attempt > 2""",
            source_id,
            BOUNDARY_STEP,
        )
        assert beyond_reclaim == 0, (
            f"{beyond_reclaim} record rows were claimed beyond the single reclaim (attempt > 2)"
        )
        ordinal_sum = await conn.fetchval(
            f"""SELECT sum(map_index) FROM "{schema}".jobs
                WHERE parent_id = $1 AND step_key = $2""",
            source_id,
            BOUNDARY_STEP,
        )
        assert int(ordinal_sum) == BOUNDARY_RECORDS * (BOUNDARY_RECORDS - 1) // 2, (
            f"the record ordinals are not the exact set 0..{BOUNDARY_RECORDS - 1}: sum={ordinal_sum}"
        )

        # ── THE BOUNDARY PAIR reconciles across the five carriers.
        for ordinal in (BOUNDARY_LAST_NARROW_ORDINAL, BOUNDARY_FIRST_WIDE_ORDINAL):
            row = await conn.fetchrow(
                f"""SELECT id, status::text AS status, attempt, finished_at, trace_id,
                           idempotency_key
                    FROM "{schema}".jobs
                    WHERE parent_id = $1 AND step_key = $2 AND map_index = $3::bigint""",
                source_id,
                BOUNDARY_STEP,
                ordinal,
            )
            assert row is not None, f"ordinal {ordinal} has no jobs row"
            assert row["status"] == "succeeded", f"ordinal {ordinal}: {dict(row)}"
            assert int(row["attempt"]) in (1, 2), f"ordinal {ordinal}: {dict(row)}"
            assert row["finished_at"] is not None, f"ordinal {ordinal}: {dict(row)}"
            assert row["trace_id"] == f"boundary-{ordinal}", dict(row)
            # exactly one row across the live table and the archive
            copies = await conn.fetchval(
                f"""SELECT (SELECT count(*) FROM "{schema}".jobs WHERE id = $1)
                         + (SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1)""",
                row["id"],
            )
            assert copies == 1, f"ordinal {ordinal} exists {copies} times across jobs/archive"
            # The ledger: EXACTLY one SUCCEEDED terminal for this identity,
            # at the record row's own attempt (the terminal write rides the
            # finalize); any other row for the identity is the corpse's
            # FENCED phantom attempt, never a second success and never a lie.
            ledger = await conn.fetch(
                f"""SELECT status, attempt FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1 AND step_key = $2 AND map_index = $3::bigint
                    ORDER BY attempt""",
                flow_id,
                BOUNDARY_STEP,
                ordinal,
            )
            successes = [r for r in ledger if r["status"] == "succeeded"]
            assert len(successes) == 1, (
                f"ordinal {ordinal} has {len(successes)} succeeded ledger rows: {ledger}"
            )
            assert int(successes[0]["attempt"]) == int(row["attempt"]), (
                f"ordinal {ordinal}: the job's attempt {row['attempt']} is not the "
                f"ledger's succeeded attempt: {ledger}"
            )
            assert all(r["status"] in ("succeeded", "fenced") for r in ledger), (
                f"ordinal {ordinal} carries a ledger row that is neither a success "
                f"nor the corpse's fenced phantom: {ledger}"
            )
            # no join-delivery row and no hold row can name these records
            outbox = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".wf_outbox WHERE join_job_id = $1', row["id"]
            )
            assert outbox == 0, f"ordinal {ordinal} has {outbox} outbox delivery rows"
        signals = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_signals WHERE workflow_id = $1', flow_id
        )
        assert signals == 0, f"the boundary run minted {signals} hold rows"

        # ── the crashed attempt's ledger row is the FENCED phantom the sweep
        # reaps on a terminal flow — never a running row left forever, never a
        # terminal lie.
        async def _phantom_fenced() -> bool:
            row = await conn.fetchrow(
                f"""SELECT count(*)::int AS rows,
                           count(*) FILTER (WHERE status = 'fenced')::int AS fenced,
                           max(attempt)::int AS maximum
                    FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1 AND step_key = 'source'""",
                flow_id,
            )
            assert row is not None
            return int(row["maximum"] or 0) == 2 and int(row["fenced"]) == 1

        measured = await _wait_for(
            _phantom_fenced,
            interval_s=0.25,
            deadline_s=_PHANTOM_FENCE_BOUND_S,
            what="the crashed source attempt's ledger row fenced by the terminal sweep",
        )
        print(
            f"[boundary] phantom-fence: measured={measured:.2f}s bound={_PHANTOM_FENCE_BOUND_S:.2f}s"
        )
        source_ledger = await conn.fetch(
            f"""SELECT status, attempt FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1 AND step_key = 'source' ORDER BY attempt, claim_epoch""",
            flow_id,
        )
        assert [(r["attempt"], r["status"]) for r in source_ledger] == [
            (1, "fenced"),
            (2, "succeeded"),
        ], [dict(r) for r in source_ledger]

        # ── the run's own ledger reconciles and the invariants close.
        record_ledger = await conn.fetchrow(
            f"""SELECT count(*) FILTER (WHERE status = 'succeeded')::bigint AS successes,
                       count(DISTINCT map_index) FILTER (WHERE status = 'succeeded')::bigint
                           AS identities,
                       count(*) FILTER (WHERE status NOT IN ('succeeded', 'fenced'))::bigint
                           AS lies
                FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1 AND step_key = $2""",
            flow_id,
            BOUNDARY_STEP,
        )
        assert record_ledger is not None
        assert int(record_ledger["successes"]) == BOUNDARY_RECORDS, record_ledger
        assert int(record_ledger["identities"]) == BOUNDARY_RECORDS, record_ledger
        assert int(record_ledger["lies"]) == 0, (
            f"the record ledger carries rows that are neither successes nor the "
            f"corpse's fenced phantoms: {record_ledger}"
        )
        # ── THE FENCE LEG: the corpse's ledger-claimed subset must ALL be
        # fenced (zero lost phantoms) and no corpse row fenced beyond its
        # own ledger claims (a claimed-but-unledgered row has no phantom to
        # fence — status-unfiltered, snapshotted (job_id, attempt) pairs,
        # never joined to post-reclaim jobs.attempt). The subset was
        # asserted non-empty at the kill; re-assert it here so the equality
        # can never be ∅ == ∅.
        fenced_ids = {
            str(r["job_id"])
            for r in await conn.fetch(
                f"""SELECT job_id FROM "{schema}".wf_step_ledger
                    WHERE flow_id = $1 AND step_key = $2 AND status = 'fenced'""",
                flow_id,
                BOUNDARY_STEP,
            )
        }
        assert len(corpse_ledger_claims) > 0, (
            "the corpse's wf_step_ledger claim subset is empty at the fence "
            "leg: the equality below would be vacuous"
        )
        print(
            f"[boundary] fence sets: corpse_records={len(corpse_records)} "
            f"ledger-claimed={len(corpse_ledger_claims)} fenced_ids={len(fenced_ids)}"
        )
        assert (fenced_ids & set(corpse_records)) == {
            job_id for job_id, _ in corpse_ledger_claims
        }, (
            f"the fenced ledger phantoms are not exactly the corpse's "
            f"ledger-claimed subset within the corpse's rows: "
            f"lost phantoms="
            f"{sorted({job_id for job_id, _ in corpse_ledger_claims} - fenced_ids)}, "
            f"fenced beyond the corpse's ledger claims="
            f"{sorted(fenced_ids & set(corpse_records) - {job_id for job_id, _ in corpse_ledger_claims})}"
        )
        # NO BURIED PHANTOM: every fenced row is a claim the run moved past
        # and RE-EXECUTED — a fence on a never-re-executed row is a lost
        # record wearing a fence. (The machinery's own re-claims fence
        # beside the corpse's: their abandoned attempt-1 claims are
        # fenceable at the terminal flow, so fenced_ids ⊆ retried, with the
        # corpse's subset asserted above and the terminals deduped below.)
        unexecuted_fences = fenced_ids - retried
        assert not unexecuted_fences, (
            f"{len(unexecuted_fences)} fenced ledger phantoms name rows the "
            f"run never re-executed — a buried claim, not a fenced one: "
            f"{sorted(unexecuted_fences)}"
        )

        # ── THE CENSUS: re-stamp immediately before the invariant read (the
        # harness's contract — the tag is a census, not a birthright; emitted
        # children do not inherit the source's tag) and assert the stamp's
        # coverage: the tagged population equals the run's row count (the
        # root, the source and 32,769 records), both numbers printed.
        await tag_run_rows(pool, schema, flow_id, _TAG)
        census = await conn.fetchrow(
            f"""SELECT count(*)::bigint AS run_rows,
                       count(*) FILTER (WHERE tags @> ARRAY[$2::text])::bigint AS tagged
                FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
            flow_id,
            _TAG,
        )
        assert census is not None and census["run_rows"] == census["tagged"] > 0, (
            f"the tagged census does not cover the run's rows: {dict(census) if census else None}"
        )
        print(
            f"[boundary] census coverage: tagged={census['tagged']} "
            f"of run_rows={census['run_rows']}"
        )
        await assert_balanced(sys_ledger, schema, _TAG)
        print("[boundary] invariants: balanced")
    finally:
        await _stop_fleet(fleet)
        with contextlib.suppress(asyncpg.PostgresError):
            # Why: the schema may already be gone on a failed module teardown.
            await clear_e2e_release_gate(conn, schema, BOUNDARY_GATE)
        await conn.close()
        await pool.close()
