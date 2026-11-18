"""The contract §9 control 4: the committed threshold survives a SIGKILL and
recovers on a second real PID.

The contract's sentence, satisfied window by window: *pause after
terminal+generation COMMIT before drain, SIGKILL, elect a different real
PID, and prove one reservation/claim from unchanged cursor, exact
children, no duplicates, preserved ``awaited``, and no logical-attempt
spend. Cover park-before-ack, rollback, ambiguous commit, mid-chunk kill,
notification suppression/loss, and pre-register actual initial+replacement
``R`` before execution.*

THE DETERMINISTIC SEAMS (no sleeping and hoping):

* **The committed-threshold window** — the product's own flow admission
  lock. The drain transaction's first contention is
  ``pg_advisory_xact_lock(hashtext(flow_id))``; the terminal's finalize
  transaction and the park transaction NEVER take it. The control holds
  the SESSION-level lock: the terminal + generation COMMIT lands, every
  drain arm of the producing pod blocks BEFORE the promotion CAS, and the
  intent is durably pending — the exact pause point, observed in SQL, no
  product seam required.
* **The park-before-ack window** — the same seam the tier's existing
  ``test_sigkill_after_park_before_postcheck_converges_without_source_retry``
  uses: the producing pod parks and blocks inside the post-park root
  check on the control's ``FOR UPDATE`` of the flow's root row; the
  park's own ack never happens; the control kills there and proves the
  marker plus the refund survived, then that exactly one later promotion
  completes the run with no attempt inflation.
* **The rollback window** — the same admission lock, held while the
  source's EMIT transaction is in flight: the kill rolls the emit back
  (nothing durable), and the control proves no park ever committed, the
  reclaim runs once (the physical attempt's documented +1), and the
  logical attempt never spends.
* **The mid-chunk window** — the product gives no deterministic pause
  between a full ``FLOW_DRAIN_MAX_WINS`` chunk commit and the next
  chunk's own first observation, so this control adds the CONTRACT'S
  test-only fault injection point: ``TASKQ_TEST_DRAIN_CHUNK_PAUSE_S``, an
  env-gated hook inside the drain's NEXT chunk transaction (after a
  previous chunk of the same invocation committed its wins), documented
  test-only in :mod:`taskq.workflows._emit` and never a production
  default. The hook holds the flow's admission lock while paused, so the
  WHOLE estate freezes with the first chunk's wins committed and the
  generation pending; the control SIGKILLs the draining pod there.
* **Ambiguous commit** is the committed-threshold window itself (the
  commit landed, the caller never learned the drain's outcome): the
  recovery reconciles from the ROWS — asserted by the unchanged cursor,
  the exact children, and zero duplicate emits (no re-emit).
* **Notification suppression/loss** — the killed pod's promotion notify
  never fires (the drain never ran) and its own listener arms die with
  the process; the replacement PID recovers from the DURABLE intent with
  no notification participating.

THE MANY-WAITER ESTATE (the mid-chunk arm's flow): 400 park rows are
SEEDED at the flow's constructible bound — ``waiters * (1 + W)`` = 800
with W=1, the app's own declared ``max_in_flight`` and every seeded
marker/intent bound — so a 201st candidate still fits at the 200-win
chunk ceiling: the released drain MUST chunk (chunk 1 commits 200 wins,
the generation stays pending, and chunk 2 opens under it).

THE R PRE-REGISTRATION (printed before execution, never changed after
observation): the victim pod's initial leadership assumption R=1 and the
replacement pod's own assumption R=1 — R=2 per scenario. The measured
replacement census is printed beside it.
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
from __future__ import annotations

import asyncio
import json as _json
import time
from pathlib import Path
from typing import Any, Final

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows.ledger import TERMINAL_JOB_STATUSES
from tests.system_e2e import _p2_saturation_worker as entry

pytestmark = [pytest.mark.system, pytest.mark.integration]

_KILL_WAIT_S = 10.0
_QUEUES = f"{entry.SOURCE_QUEUE},{entry.LEAF_QUEUE},{entry.SIBLING_QUEUE}"

#: THE RUNS' OWN EXPECTED CHILD SETS, derived from the estates' own page
#: declarations (the bodies' one truth — never a re-counted literal): the
#: readiness predicates below compare the durable children rows against
#: THESE sets, so a predicate and the assertion that follows it can never
#: drift apart about how many children the run owes.
_C4_KILL_CHILDREN: Final[frozenset[int]] = frozenset(
    m for page in entry.C4_KILL_PAGES for m in page
)
_C4_PARK_CHILDREN: Final[frozenset[int]] = frozenset(
    m for page in entry.C4_PARK_PAGES for m in page
)


def _children_settled(kids: list[dict[str, Any]], expected: frozenset[int]) -> bool:
    """The completeness leg every recovery wait gates on: the durable
    children rows' map-index set equals the run's own expected child set
    EXACTLY, and every row is in a terminal status (the state machine's
    own vocabulary, never a re-spelled literal).

    NOT merely "none pending": a page the recovery never emitted would
    leave its child row MISSING — "none pending" over the surviving rows
    would pass an incomplete recovery, so the SET must match too."""
    indices = frozenset(int(r["m"]) for r in kids)
    return indices == expected and all(r["status"] in TERMINAL_JOB_STATUSES for r in kids)


def _admission_lock_sql() -> str:
    """The drain's own advisory lock, session-scoped for the control's
    connection: hashtext of the flow id — the same key the drain's
    ``pg_advisory_xact_lock`` requests."""
    return "SELECT pg_advisory_lock(hashtext($1::text))"


def _admission_unlock_sql() -> str:
    return "SELECT pg_advisory_unlock(hashtext($1::text))"


async def _flow_counts(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, int]:
    row = await pool.fetchrow(
        f"""SELECT
            count(*) FILTER (WHERE status = 'scheduled'
                             AND metadata @> '{{"blocking_reason":"backpressure"}}') AS parked,
            count(*) FILTER (WHERE status = 'pending'
                             AND metadata ? 'wf_emit_reservation') AS promoted,
            count(*) FILTER (WHERE status = 'running') AS running,
            count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
            count(*) FILTER (WHERE status = 'failed') AS failed
            FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
    )
    assert row is not None
    return {key: int(row[key]) for key in ("parked", "promoted", "running", "succeeded", "failed")}


async def _wait_for(predicate: Any, interval_s: float, deadline_s: float, description: str) -> None:
    deadline = time.monotonic() + deadline_s
    while True:
        if await predicate():
            return
        assert time.monotonic() < deadline, f"the wait expired: {description}"
        await asyncio.sleep(interval_s)


async def _source_row(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"""SELECT id, status::text AS status, attempt::int AS attempt, metadata
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
        flow_id,
    )
    assert row is not None, "the flow's source row is missing"
    return dict(row)


async def _intent_row(pool: asyncpg.Pool, schema: str, flow_id: str) -> dict[str, int]:
    row = await pool.fetchrow(
        f'SELECT bound, wake_generation, handled_generation FROM "{schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow_id,
    )
    assert row is not None, "the flow's capacity intent record is missing"
    return {
        "bound": int(row["bound"]),
        "wake": int(row["wake_generation"]),
        "handled": int(row["handled_generation"]),
    }


def _meta(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["metadata"]
    return _json.loads(raw) if isinstance(raw, str) else dict(raw)


async def _children(pool: asyncpg.Pool, schema: str, source_id: str) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        f"""SELECT map_index::int AS m, status::text AS status, idempotency_key
            FROM "{schema}".jobs WHERE parent_id = $1::uuid ORDER BY map_index""",
        source_id,
    )
    return [dict(r) for r in rows]


async def _source_ledger(pool: asyncpg.Pool, schema: str, flow_id: str) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        f"""SELECT status::text AS status, attempt::int AS attempt
            FROM "{schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY created_at""",
        flow_id,
    )
    return [dict(r) for r in rows]


async def _stop(proc: Any) -> None:
    if proc.proc.poll() is None:
        proc.proc.kill()
        await asyncio.to_thread(proc.proc.wait)


async def _spawn_pod(
    conn: asyncpg.Connection,
    pg_dsn: str,
    schema: str,
    log_dir: Any,
    tag: str,
    *,
    chunk_pause: bool = False,
) -> Any:
    from tests.system_e2e._harness import spawn_joined_worker

    extra_env = {
        "TASKQ_QUEUES": _QUEUES,
        "TASKQ_MAX_CONCURRENCY": "8" if not chunk_pause else "32",
    }
    if chunk_pause:
        # THE CONTRACT'S TEST-ONLY FAULT INJECTION POINT (the mid-chunk
        # window's seam): never a production default.
        extra_env["TASKQ_TEST_DRAIN_CHUNK_PAUSE_S"] = "45"
    return await spawn_joined_worker(
        conn,
        pg_dsn,
        schema,
        tag=tag,
        entry="tests.system_e2e._p2_saturation_worker",
        log_sink=str(log_dir / f"p2-system-{tag}.worker.log"),
        extra_env=extra_env,
    )


async def _census_of(log_path: Any) -> dict[str, int]:
    """The pod's own last census snapshot (its periodic WARNING line)."""
    content = await asyncio.to_thread(log_path.read_text)
    lines = [ln for ln in content.splitlines() if '"p2sat-census"' in ln]
    assert lines, f"the pod never logged its census snapshot ({log_path})"
    raw = _json.loads(lines[-1])
    return {key: int(value) for key, value in raw.items() if type(value) is int}


async def _census_grants_at_least(
    log_path: Any, min_grants: int, deadline_s: float
) -> dict[str, int]:
    """Wait for the pod's own periodic census (the 10s cadence; the
    control kills the pod before its shutdown snapshot) to show at least
    *min_grants* fitted grants — the replacement's own arms ran the
    promotion."""
    deadline = time.monotonic() + deadline_s
    last: dict[str, int] = {}
    while True:
        try:
            last = await _census_of(log_path)
            if last.get("fitted_grants", 0) >= min_grants:
                return last
        except AssertionError:
            pass
        assert time.monotonic() < deadline, (
            f"the pod's census never showed {min_grants} fitted grants: {last}"
        )
        await asyncio.sleep(1.0)


def _print_r(pre_registered_r: dict[str, int]) -> None:
    print(
        "P2SAT-KILL-R-PRE-REGISTRATION " + _json.dumps(pre_registered_r, sort_keys=True),
        flush=True,
    )


async def test_committed_threshold_survives_kill_and_recovers_on_second_pid(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The committed-threshold window: the terminal + generation COMMIT
    lands, the producing pod's drain blocks at the flow's own admission
    lock (held by the control), the control SIGKILLs that pod, and a
    DIFFERENT real PID recovers with ONE promotion from the UNCHANGED
    cursor — exact children, no duplicates, the preserved ``awaited``, no
    logical-attempt spend. The same window IS the ambiguous-commit shape
    (the recovery reconciles from rows, never a re-emit) and the
    notification-suppression shape (the victim's notify never fired; the
    durable intent alone owns the recovery)."""
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    victim = replacement = None
    try:
        runner = FlowRunner(entry.p2sat_app.get("c4_kill_recovery_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)

        # THE PRE-REGISTERED R, printed BEFORE execution: the victim's
        # initial leadership assumption R=1 + the replacement pod's own
        # assumption R=1. Neither counts nor labels change after
        # observation.
        pre_registered_r = {"initial": 1, "replacement": 1, "total": 2}
        _print_r(pre_registered_r)

        # The spawn runs CONCURRENTLY with the parked wait: the pod boots
        # in ~2-3s and the park lives only until the children's gates end —
        # the control must hold the admission lock BEFORE the children's
        # terminals fire their drains.
        spawn_task = asyncio.create_task(
            _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-kill-victim")
        )

        # The source parks on its page-1 emit (page 0's two children hold
        # the bound); the victim's pod claimed it.
        async def _parked() -> bool:
            row = await _source_row(pool, schema, flow_id)
            return row["status"] == "scheduled"

        await _wait_for(_parked, 0.05, 180, "the source never parked")

        # THE SEAM, taken AT the parked observation (the pod is still
        # booting; the page-1 emit refused at the pre-check, so no
        # transaction needs the lock any more): the drain's first
        # contention; the terminal's finalize tx never takes it. The
        # park's own trigger drain has already completed (no fitting
        # waiter at bound); the lock lands BEFORE the children's 3s gates
        # end, so their terminals' re-arm drains block here.
        await conn.execute(_admission_lock_sql(), flow_id)
        victim = await spawn_task

        # The committed-threshold state: child 0's terminal + generation
        # COMMIT landed (its body's 3s gate ends), the intent re-armed and
        # durably PENDING (wake > handled), the drain blocked before any
        # promotion CAS.
        async def _window() -> bool:
            counts = await _flow_counts(pool, schema, flow_id)
            intent = await _intent_row(pool, schema, flow_id)
            return counts["succeeded"] >= 1 and intent["wake"] > intent["handled"]

        await _wait_for(_window, 0.1, 120, "the committed-threshold window never opened")

        # THE KILL: a real SIGKILL inside the window.
        victim_pid = victim.proc.pid
        victim.proc.kill()
        await asyncio.to_thread(victim.proc.wait, _KILL_WAIT_S)
        assert victim.proc.returncode == -9, victim.proc.returncode

        # THE POST-KILL ESTATE: the committed generation survived; the
        # drain never ran. The cursor is UNCHANGED (page 0 — the resume
        # never happened), the children are EXACTLY page 0's two (page
        # 1's record was never emitted — no duplicate emit), no promotion
        # happened (wakes == 0), the parked claim's ledger is intact
        # (awaited, attempt 1), and the logical attempt never spent.
        intent = await _intent_row(pool, schema, flow_id)
        assert intent["wake"] > intent["handled"], intent
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        pressure = meta["wf_emit_backpressure"]
        assert source["status"] == "scheduled", source
        assert pressure["deferrals"] == 1, pressure
        assert int(pressure.get("wakes", 0)) == 0, pressure
        assert meta.get("emit_cursor", {}).get("page") == 0, meta
        assert meta["wf_source_logical_attempt"] == 1, meta
        children = await _children(pool, schema, str(source["id"]))
        assert [int(r["m"]) for r in children] == [0, 1], children
        ledgers = await _source_ledger(pool, schema, flow_id)
        assert [r["status"] for r in ledgers] == ["awaited"], ledgers
        assert [int(r["attempt"]) for r in ledgers] == [1], ledgers

        # THE SECOND PID: release the lock; the replacement recovers.
        await conn.execute(_admission_unlock_sql(), flow_id)
        replacement = await _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-kill-replacement")
        assert replacement.proc.pid != victim_pid, "the replacement must be a DIFFERENT real PID"

        # THE READINESS LAW (the round-19 flake's own lesson): the wait
        # completes only when EVERY fact the assertions below read is
        # stable — the source succeeded AND the durable children rows
        # settle onto the run's own expected child set (exactly, terminal)
        # AND the drain's generation is fully handled. The source's own
        # success is NOT that state: its body returns after the final
        # page's emit, so the source can terminalise while the last
        # child is still pending (the source-success instant is the
        # round-19 red's exact race), and the last child's terminal
        # re-arms the intent after it — each fact lands in its own
        # transaction, so each must be waited for.
        async def _done() -> bool:
            row = await _source_row(pool, schema, flow_id)
            if row["status"] != "succeeded":
                return False
            kids = await _children(pool, schema, str(row["id"]))
            if not _children_settled(kids, _C4_KILL_CHILDREN):
                return False
            intent = await _intent_row(pool, schema, flow_id)
            return intent["handled"] == intent["wake"]

        await _wait_for(_done, 0.2, 300, "the replacement never completed the run")

        # THE RECOVERY'S OWN ACCOUNTING: exactly ONE promotion from the
        # unchanged cursor; the exact three children ONCE each (no
        # duplicates — a re-emit would duplicate); the preserved
        # `awaited` beside the resumed claim's terminal; no
        # logical-attempt spend.
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        pressure = meta["wf_emit_backpressure"]
        assert source["status"] == "succeeded"
        assert pressure["deferrals"] == 1, pressure
        assert int(pressure["wakes"]) == 1, pressure
        assert meta.get("emit_cursor", {}).get("page") == 1, meta
        assert meta["wf_source_logical_attempt"] == 1, meta
        intent = await _intent_row(pool, schema, flow_id)
        assert intent["handled"] == intent["wake"], intent
        children = await _children(pool, schema, str(source["id"]))
        assert [int(r["m"]) for r in children] == [0, 1, 2], children
        assert {r["status"] for r in children} == {"succeeded"}, children
        assert len({r["idempotency_key"] for r in children}) == 3
        ledgers = await _source_ledger(pool, schema, flow_id)
        assert [r["status"] for r in ledgers] == ["awaited", "succeeded"], ledgers
        assert [int(r["attempt"]) for r in ledgers] == [1, 1], ledgers
        counts = await _flow_counts(pool, schema, flow_id)
        assert counts["failed"] == 0, counts

        # THE REPLACEMENT'S OWN CENSUS: its arms ran the promotion (the
        # pod's own 10s-cadence snapshot; the control kills the pod
        # before its shutdown line).
        census = await _census_grants_at_least(
            log_dir / "p2-system-c4-kill-replacement.worker.log", 1, 30.0
        )
        assert census.get("flow_drains", 0) >= 1, census
        print(
            f"P2SAT-KILL-RECOVERED victim_pid={victim_pid} "
            f"replacement_pid={replacement.proc.pid} R={pre_registered_r['total']} "
            f"census={_json.dumps(census, sort_keys=True)}",
            flush=True,
        )
    finally:
        if victim is not None:
            await _stop(victim)
        if replacement is not None:
            await _stop(replacement)
        await conn.close()
        await pool.close()


async def test_park_before_ack_kill_preserves_marker_and_promotes_once(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The park-before-ack window: the producing pod parks the source and
    blocks inside the post-park root check on the control's ``FOR UPDATE``
    of the flow's root row — the park's own ack never happens. The
    SIGKILL there leaves the committed marker + the refunded attempt
    durable; the replacement PID recovers with exactly one later
    promotion and no attempt inflation beyond the reclaim's own
    documented rule."""
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    blocker_tx: Any = None
    victim = replacement = None
    try:
        runner = FlowRunner(entry.p2sat_app.get("c4_park_kill_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)

        pre_registered_r = {"initial": 1, "replacement": 1, "total": 2}
        _print_r(pre_registered_r)

        spawn_task = asyncio.create_task(
            _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-park-victim")
        )

        async def _parked() -> bool:
            row = await _source_row(pool, schema, flow_id)
            return row["status"] == "scheduled"

        await _wait_for(_parked, 0.05, 180, "the source never parked")

        # THE SEAM, taken AT the parked observation (the pod is still
        # booting): hold the flow's ROOT row — the pod's post-park root
        # check blocks on it, AFTER the park's commit and before the ack.
        blocker_tx = conn.transaction()
        await blocker_tx.start()
        root_id = await conn.fetchval(
            f'SELECT id FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='__flow__' "
            "FOR UPDATE",
            flow_id,
        )
        assert root_id is not None
        victim = await spawn_task

        # THE KILL inside the park-before-ack window.
        victim_pid = victim.proc.pid
        victim.proc.kill()
        await asyncio.to_thread(victim.proc.wait, _KILL_WAIT_S)
        assert victim.proc.returncode == -9, victim.proc.returncode
        await blocker_tx.rollback()
        blocker_tx = None

        # THE POST-KILL ESTATE: the park's marker + the refunded attempt
        # are durable; no promotion ever ran.
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        pressure = meta["wf_emit_backpressure"]
        assert source["status"] == "scheduled", source
        assert pressure["deferrals"] == 1, pressure
        assert int(pressure.get("wakes", 0)) == 0, pressure
        assert meta.get("emit_cursor", {}).get("page") == 0, meta
        assert meta["wf_source_logical_attempt"] == 1, meta
        ledgers = await _source_ledger(pool, schema, flow_id)
        # THE PRESERVED 'AWAITED' (the contract's own word): the park's
        # own rest is the claimed attempt's ledger row at 'awaited' —
        # exactly one row, exactly attempt 1, never 'failed', never a
        # spend, never the zombie fence's 'fenced' (a live claimant's
        # park is not a zombie's).
        assert [r["status"] for r in ledgers] == ["awaited"], ledgers
        assert [int(r["attempt"]) for r in ledgers] == [1], ledgers

        replacement = await _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-park-replacement")
        assert replacement.proc.pid != victim_pid

        # THE READINESS LAW (the same one the committed-threshold control
        # states): the assertions below read the source's estate, the
        # run's children (through the whole-flow failed count) and the
        # drain's handled generation — so the wait completes only when
        # the source succeeded, every child of the run's own declared
        # page set is terminal on exactly that set, and the intent is
        # fully handled. The source's success alone is a subset gate: its
        # body returns after the final page's emit, while the last child
        # and the last drain generation are still in flight.
        async def _done() -> bool:
            row = await _source_row(pool, schema, flow_id)
            if row["status"] != "succeeded":
                return False
            kids = await _children(pool, schema, str(row["id"]))
            if not _children_settled(kids, _C4_PARK_CHILDREN):
                return False
            intent = await _intent_row(pool, schema, flow_id)
            return intent["handled"] == intent["wake"]

        await _wait_for(_done, 0.2, 300, "the replacement never completed the run")

        # THE RECOVERY: exactly one later promotion; no attempt inflation
        # beyond the reclaim's own documented rule (the source's physical
        # attempt stays 1 — the park's refund stands).
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        pressure = meta["wf_emit_backpressure"]
        assert source["status"] == "succeeded"
        assert int(source["attempt"]) == 1, source
        assert pressure["deferrals"] == 1, pressure
        assert int(pressure["wakes"]) == 1, pressure
        assert meta["wf_source_logical_attempt"] == 1, meta
        intent = await _intent_row(pool, schema, flow_id)
        assert intent["handled"] == intent["wake"], intent
        ledgers = await _source_ledger(pool, schema, flow_id)
        # THE RECOVERY'S OWN PAIR: the preserved 'awaited' rest beside the
        # resumed claim's own succeeded row (the new epoch's own row — the
        # parked attempt's record is never overwritten).
        assert [r["status"] for r in ledgers] == ["awaited", "succeeded"], ledgers
        assert [int(r["attempt"]) for r in ledgers] == [1, 1], ledgers
        counts = await _flow_counts(pool, schema, flow_id)
        assert counts["failed"] == 0, counts
        census = await _census_grants_at_least(
            log_dir / "p2-system-c4-park-replacement.worker.log", 1, 30.0
        )
        print(
            f"P2SAT-PARK-KILL-RECOVERED victim_pid={victim_pid} "
            f"replacement_pid={replacement.proc.pid} R={pre_registered_r['total']} "
            f"census={_json.dumps(census, sort_keys=True)}",
            flush=True,
        )
    finally:
        if blocker_tx is not None:
            await blocker_tx.rollback()
        if victim is not None:
            await _stop(victim)
        if replacement is not None:
            await _stop(replacement)
        await conn.close()
        await pool.close()


async def test_emit_rollback_kill_never_parks_and_reclaims_once(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The rollback window (the pre-commit face): the control holds the
    flow's admission lock while the source's EMIT transaction is in
    flight; the SIGKILL rolls the emit back — nothing durable, no park —
    and the replacement's reclaim reruns ONCE (the physical attempt's
    documented +1, the logical attempt unspent) and completes the run."""
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    victim = replacement = None
    try:
        runner = FlowRunner(entry.p2sat_app.get("c4_kill_recovery_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)

        pre_registered_r = {"initial": 1, "replacement": 1, "total": 2}
        _print_r(pre_registered_r)

        # THE SEAM FIRST: the admission lock held from before the spawn —
        # the source's CLAIM needs no lock (it proceeds), but its page-0
        # EMIT's transaction contends here and blocks in flight.
        await conn.execute(_admission_lock_sql(), flow_id)

        spawn_task = asyncio.create_task(
            _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-roll-victim")
        )

        async def _claimed() -> bool:
            row = await _source_row(pool, schema, flow_id)
            return row["status"] == "running"

        await _wait_for(_claimed, 0.05, 120, "the source never claimed")
        victim = await spawn_task

        # THE KILL inside the emit's transaction window.
        victim_pid = victim.proc.pid
        victim.proc.kill()
        await asyncio.to_thread(victim.proc.wait, _KILL_WAIT_S)
        assert victim.proc.returncode == -9, victim.proc.returncode

        # THE ROLLBACK'S DURABLE FACE: the emit never committed — no park
        # marker, no backpressure metadata, no cursor checkpoint, no
        # children rows.
        await conn.execute(_admission_unlock_sql(), flow_id)
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        assert "wf_emit_backpressure" not in meta, meta
        assert "emit_cursor" not in meta, meta
        children_after_kill = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE parent_id = $1::uuid',
            source["id"],
        )
        assert int(children_after_kill) == 0, children_after_kill

        replacement = await _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-roll-replacement")
        assert replacement.proc.pid != victim_pid

        # THE READINESS LAW (the round-19 red's own control): the source
        # is the flow's EMITTER — its body returns after the final page's
        # emit and its own terminal lands in its own transaction, while
        # the page's children are still being claimed and terminalised
        # (the round-19 flake read child m=2 PENDING at the source-success
        # instant). The assertions below read the children's terminal set
        # and the whole-flow failed count, so the wait completes only
        # when the source succeeded AND the durable children rows have
        # settled onto the run's own expected child set — exactly, and
        # terminal.
        async def _done() -> bool:
            row = await _source_row(pool, schema, flow_id)
            if row["status"] != "succeeded":
                return False
            kids = await _children(pool, schema, str(row["id"]))
            return _children_settled(kids, _C4_KILL_CHILDREN)

        await _wait_for(_done, 0.5, 300, "the reclaim never completed the run")

        # THE RECLAIM'S OWN ACCOUNTING: rerun ONCE (the physical attempt's
        # documented +1), the logical attempt never spent, the exact three
        # children once each.
        source = await _source_row(pool, schema, flow_id)
        meta = _meta(source)
        assert source["status"] == "succeeded"
        # The workflow row's disown RE-MINTS the physical attempt (the
        # control-5 refund/re-mint rule): the reclaim re-claims at the
        # same attempt 1 — the ledger pair below is the evidence; the
        # logical attempt never spends.
        assert int(source["attempt"]) == 1, source
        assert meta["wf_source_logical_attempt"] == 1, meta
        assert meta.get("emit_cursor", {}).get("page") == 1, meta
        pressure = meta.get("wf_emit_backpressure")
        if pressure is not None:
            # The run's own page-1 park happened on the reclaim's normal
            # path; the recovery's promotion is its own single wake.
            assert pressure["deferrals"] == 1, pressure
            assert int(pressure["wakes"]) == 1, pressure
        children = await _children(pool, schema, str(source["id"]))
        assert [int(r["m"]) for r in children] == [0, 1, 2], children
        assert {r["status"] for r in children} == {"succeeded"}, children
        assert len({r["idempotency_key"] for r in children}) == 3
        counts = await _flow_counts(pool, schema, flow_id)
        assert counts["failed"] == 0, counts
        print(
            f"P2SAT-ROLLBACK-RECLAIMED victim_pid={victim_pid} "
            f"replacement_pid={replacement.proc.pid} R={pre_registered_r['total']}",
            flush=True,
        )
    finally:
        if victim is not None:
            await _stop(victim)
        if replacement is not None:
            await _stop(replacement)
        await conn.close()
        await pool.close()


async def test_mid_chunk_kill_completes_from_committed_wins_and_real_rows(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """The mid-chunk window over the seeded 400-waiter estate.

    THE ESTATE'S OWN PREMISE (the constructible bound, the flow_600
    precedent): 400 waiters at width W=1 with the constructible bound
    ``waiters * (1 + W)`` = 800 — the app's own declared
    ``max_in_flight`` and every seeded marker/intent bound. The bound
    must leave a 201st candidate FITTING at the 200-win ceiling (the
    200th win's own charge is the live source plus 200 promoted rows
    with reservations): then the ceiling transaction commits its 200
    wins, leaves the generation pending, and opens chunk 2 — the
    freeze/kill window the control exists to test actually exists. A
    tighter bound (the estate's original 400) exhausts admission right
    after the 200th win, so the drain's final observation finds no
    fitting candidate and completes the invocation in ONE transaction —
    the freeze never lands and the control reds on its own premise, not
    on the product.

    THE INTENT IS SEEDED PENDING (wake 5 > handled 2, bound 800): the
    pod's leadership backfill is the drain's FIRST and only racing arm.
    With an engine-created intent the source's own terminal event fires
    the terminal route's drain trigger CONCURRENTLY with the backfill,
    and the second arm's chunk 1 consumes the unpromoted half within a
    second of the first arm's chunk 1 — the 200-parked state is a
    sub-second transient no poll can observe. Seeding the intent
    pending makes the source's terminal event a no-op (the intent is
    already pending — nothing created, nothing notified), the freeze
    plateau at exactly 200 parked rows lasts as long as the hook
    sleeps, and the control observes it.

    The estate's park rows are SEEDED, never engine-born (the §9.2
    pattern: the complete §2.2 metadata as exact literal values, no other
    keys), so the estate exists COMPLETE before any pod boots — no park
    race, no self-draining trigger cascade. The real drain over the real
    pods MUST chunk: chunk 1 promotes 200 waiters to COMMIT; the
    contract's test-only hook (``TASKQ_TEST_DRAIN_CHUNK_PAUSE_S``) then
    pauses inside chunk 2's transaction holding the flow's admission
    lock — the estate freezes with the committed wins and the pending
    generation; the SIGKILL there leaves exactly that state durable, and
    the replacement PID completes the estate from real rows — every A
    succeeded exactly once, the intent handled, zero failures."""
    from taskq._ids import new_uuid
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    victim = replacement = None
    try:
        # The A bodies' own effect gate: the seeded estate's gate rows
        # are pre-stamped (the seeded A's "already ran their first
        # runs"), so each A's first claim after promotion completes.
        await pool.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".p2sat_c4_gate ('
            "flow_id uuid NOT NULL, map_index bigint NOT NULL, "
            "PRIMARY KEY (flow_id, map_index))"
        )
        runner = FlowRunner(entry.p2sat_app.get("c4_seed_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)
        source_id = str(
            await pool.fetchval(
                f'SELECT id FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='source'",
                flow_id,
            )
        )

        # THE SEEDED ESTATE: 400 exact-§2.2 park rows (width 1, bound
        # 800 = the constructible waiters*(1+W), deferrals 1, wakes 0) +
        # their gate rows + THE PENDING INTENT RECORD, SEEDED PENDING
        # (bound 800 — the app's own declared max_in_flight — wake 5 >
        # handled 2). Seeding the intent PENDING is the premise that
        # makes the freeze plateau observable: the estate is COMPLETE
        # before any pod boots (no park race, no trigger cascade), the
        # pod's leadership backfill is the drain's first arm, and the
        # source's own terminal event creates NO second intent (the
        # intent is already pending), so the terminal route's own drain
        # trigger never fires a second concurrent drain that would race
        # the first arm's chunk 2 and consume the unpromoted half
        # before the freeze could be observed. The 201st candidate fits
        # at the 200-win ceiling, so chunk 2 exists and the freeze/kill
        # window lands there.
        deferred_at = "2026-11-15T09:30:00.123456Z"
        chunk_bound = entry.CHUNK_ESTATE * 2
        await pool.execute(
            f'INSERT INTO "{schema}".wf_emit_capacity '
            "(flow_id, bound, wake_generation, handled_generation) "
            "VALUES ($1::uuid, $2, 5, 2) "
            "ON CONFLICT (flow_id) DO UPDATE SET "
            "wake_generation = 5, handled_generation = 2",
            flow_id,
            chunk_bound,
        )
        seeded_intent = await pool.fetchrow(
            f"SELECT bound, wake_generation, handled_generation "
            f'FROM "{schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
            flow_id,
        )
        assert seeded_intent is not None and (
            int(seeded_intent["bound"]),
            int(seeded_intent["wake_generation"]),
            int(seeded_intent["handled_generation"]),
        ) == (chunk_bound, 5, 2), dict(seeded_intent) if seeded_intent else None
        for i in range(400):
            meta = {
                "flow_id": flow_id,
                "blocking_reason": "backpressure",
                "wf_deferral": {
                    "signal": "EmitBackpressure",
                    "consume_budget": False,
                    "attempt": 0,
                },
                "wf_emit_backpressure": {
                    "version": 1,
                    "page_width": 1,
                    "bound": chunk_bound,
                    "outstanding": chunk_bound,
                    "deferrals": 1,
                    "wakes": 0,
                    "deferred_at": deferred_at,
                },
                "wf_source_logical_attempt": 1,
            }
            await pool.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, attempt, max_attempts, "
                "retry_kind, step_key, map_index, metadata, scheduled_at, "
                "created_at, parent_id, fairness_key) "
                "VALUES ($1, 'wf', $2, $3::jsonb, 'scheduled', 0, 2, "
                "'transient', 'spawn', $4, $5::jsonb, clock_timestamp(), "
                "clock_timestamp(), $6::uuid, $7)",
                str(new_uuid()),
                entry.SOURCE_QUEUE,
                _json.dumps({"wf_item": {"app_id": i}}),
                i,
                _json.dumps(meta),
                source_id,
                flow_id,
            )
            await pool.execute(
                f'INSERT INTO "{schema}".p2sat_c4_gate (flow_id, map_index) VALUES ($1::uuid, $2)',
                flow_id,
                i,
            )
        pre_registered_r = {"initial": 1, "replacement": 1, "total": 2}
        _print_r(pre_registered_r)

        # THE VICTIM carries the test-only chunk-pause hook (the seam the
        # product lacked; documented in taskq.workflows._emit).
        victim = await _spawn_pod(
            conn,
            pg_dsn,
            schema,
            log_dir,
            "c4-chunk-victim",
            chunk_pause=True,
        )

        # THE FREEZE: the estate's real drain runs chunk 1 (200 wins) to
        # COMMIT, then the hook pauses inside chunk 2 holding ONLY the
        # flow's admission lock — the parked count freezes at exactly 200
        # (chunk 1's wins), the intent stays pending, and the promoted
        # A's claims and terminals proceed untouched.
        async def _frozen() -> bool:
            counts = await _flow_counts(pool, schema, flow_id)
            intent = await _intent_row(pool, schema, flow_id)
            return (
                counts["parked"] == 200
                and counts["succeeded"] >= 1
                and intent["wake"] > intent["handled"]
            )

        await _wait_for(_frozen, 0.1, 240, "the mid-chunk freeze never landed")
        await asyncio.sleep(1.0)  # the freeze is stable: the hook holds the lock
        after = await _flow_counts(pool, schema, flow_id)
        assert after["parked"] == 200, after
        assert after["succeeded"] >= 1, after

        # THE KILL inside the mid-chunk window.
        victim_pid = victim.proc.pid
        victim.proc.kill()
        await asyncio.to_thread(victim.proc.wait, _KILL_WAIT_S)
        assert victim.proc.returncode == -9, victim.proc.returncode

        # THE POST-KILL ESTATE: chunk 1's committed wins survived (the
        # A's already ran to their terminals through them); the chunk in
        # flight rolled back to nothing; the generation is still pending;
        # the parked estate is EXACTLY the unpromoted half.
        intent = await _intent_row(pool, schema, flow_id)
        assert intent["wake"] > intent["handled"], intent
        after = await _flow_counts(pool, schema, flow_id)
        assert after["parked"] == 200, after
        assert after["failed"] == 0, after

        replacement = await _spawn_pod(conn, pg_dsn, schema, log_dir, "c4-chunk-replacement")
        assert replacement.proc.pid != victim_pid

        # THE READINESS LAW: the assertions below read the intent's
        # handled generation beside the estate's completion. The root's
        # own `succeeded` is the engine's own all-children-terminal
        # derivation (the status reconstruction writes it only on a
        # same-transaction `NOT has_live` aggregate — every A row and the
        # source were terminal at that instant, and terminal rows have no
        # outbound transitions), so it is the authoritative completeness
        # fact for the a_rows reads. What it does NOT cover is the
        # drain's own bookkeeping: the LAST A's terminal re-arms the
        # intent in its own transaction, and the drain's
        # `handled = wake` lands concurrently with the root's finalize —
        # so the wait gates on that too, or the handled==wake assertion
        # below could read a mid-flight drain.
        async def _done() -> bool:
            root = await pool.fetchval(
                f'SELECT status::text FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='__flow__'",
                flow_id,
            )
            if root != "succeeded":
                return False
            intent = await _intent_row(pool, schema, flow_id)
            return intent["handled"] == intent["wake"]

        await _wait_for(_done, 0.5, 600, "the replacement never completed the estate")

        # THE COMPLETION FROM REAL ROWS: every A succeeded EXACTLY once
        # (no duplicate claims), the intent handled, zero failures.
        source = await _source_row(pool, schema, flow_id)
        assert source["status"] == "succeeded", source
        intent = await _intent_row(pool, schema, flow_id)
        assert intent["handled"] == intent["wake"], intent
        a_rows = await pool.fetch(
            f"""SELECT map_index::int AS m, status::text AS status, attempt::int AS a,
                       idempotency_key,
                       metadata->'wf_emit_backpressure'->>'wakes' AS wakes
                FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'spawn'
                ORDER BY map_index""",
            flow_id,
        )
        assert len(a_rows) == 400, len(a_rows)
        assert {r["status"] for r in a_rows} == {"succeeded"}, [r["status"] for r in a_rows][:5]
        # No attempt inflation beyond the reclaim's own documented rule:
        # every A ran at attempt 1, or attempt 2 when its pre-kill claim
        # was still in flight at the SIGKILL (the re-mint/reclaim pair).
        assert all(int(r["a"]) in (1, 2) for r in a_rows), [int(r["a"]) for r in a_rows][:5]
        assert sum(1 for r in a_rows if int(r["a"]) == 2) <= 32, [int(r["a"]) for r in a_rows]
        # THE PER-A EXACTLY-ONCE PROMOTION (per-row, direct): ``wakes`` is
        # the promotion CAS's own durable counter (each committed win adds
        # exactly 1, guarded by the row's reservation absence), the park
        # write preserves it, and the terminal strips only
        # ``wf_emit_reservation`` — so the count survives into the drained
        # estate, where the reservation key cannot serve (the terminal
        # removed it). This estate's A rows never re-park after promotion
        # (every gate row is pre-stamped), so a legitimate history reads
        # exactly 1 here; a double promotion confined to a ≤32-row subset
        # reads 2 on its rows — invisible to the attempt bound above (a
        # legitimate kill reclaim shows the same ≤32 attempt-2 face) and
        # to the pre-kill ``succeeded >= 1`` gate (the source alone
        # satisfies it).
        assert all(int(r["wakes"]) == 1 for r in a_rows), [
            (int(r["m"]), int(r["wakes"])) for r in a_rows if int(r["wakes"]) != 1
        ]
        counts = await _flow_counts(pool, schema, flow_id)
        assert counts["failed"] == 0, counts
        # The kill's committed-wins evidence: the A's chunk 1 promoted and
        # ran to their terminals through the frozen window (the parked
        # half stayed exactly 200).
        wins = sum(1 for r in a_rows if int(r["a"]) == 1)
        census = await _census_grants_at_least(
            log_dir / "p2-system-c4-chunk-replacement.worker.log", 1, 30.0
        )
        print(
            f"P2SAT-MIDCHUNK-RECOVERED victim_pid={victim_pid} "
            f"replacement_pid={replacement.proc.pid} R={pre_registered_r['total']} "
            f"parked_at_kill=200 committed_wins={wins} "
            f"census={_json.dumps(census, sort_keys=True)}",
            flush=True,
        )
    finally:
        if victim is not None:
            await _stop(victim)
        if replacement is not None:
            await _stop(replacement)
        await conn.close()
        await pool.close()


def families_min_ok(total: dict[str, int]) -> bool:
    """The structural minimum: the estate's own P=2 admissions (at least
    the run's own pages were admitted) — a vacuous zero-V pass is red."""
    return total.get("admission_decisions", 0) >= 2


async def test_two_worker_healthy_lane_counters_within_the_preregistered_pin(
    pg_dsn: str, module_pg_schema: ModulePgSchema, log_dir: Any
) -> None:
    """§4.2 control 3's healthy two-OS-worker lane: two real OS workers,
    the two-page/one-source healthy flow (P=2, S=1), the initial
    leadership assumption counted (R=1, no direct runner: B=0), no
    failures (J=C=0). The pre-registration prints the standalone
    acceptance caps — 60 lane against 60 general — BEFORE execution;
    the pods' own census snapshots (the WARNING-level
    ``p2sat-census`` lines) are the measurement — the fleet's V is the
    SUM of the pods' own operation instances. The red predicates: the
    measured V exceeding the lane acceptance cap 60 (and V_lane >
    V_general is impossible here — equality)."""
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)
    conn = await asyncpg.connect(pg_dsn)
    pods: list[Any] = []
    try:
        # THE PRE-REGISTRATION, printed BEFORE execution: P=2, S=1,
        # R=1 (the initial leadership assumption; no later recoveries —
        # a healthy lane), B=0 (no direct runner), J=C=0. The corrected
        # analytical envelopes (A10) and the standalone acceptance caps
        # are separately labelled: the caps are literals, never the value
        # of a current polynomial (the former 19P+18S+4R+4B+J+C aggregate
        # and its lane form are superseded historical derivations).
        pre_registered = {
            "P": 2,
            "S": 1,
            "R": 1,
            "B": 0,
            "J": 0,
            "C": 0,
            "V_general_envelope = 23P+22S+5R+5B+J+C": 23 * 2 + 22 * 1 + 5 * 1 + 5 * 0,
            "V_lane_envelope = 23P+22S+5R+B+J+C": 23 * 2 + 22 * 1 + 5 * 1 + 0,
            "acceptance cap V_lane": 60,
            "acceptance cap V_general": 60,
        }
        assert pre_registered["V_general_envelope = 23P+22S+5R+5B+J+C"] == 73, pre_registered
        assert pre_registered["V_lane_envelope = 23P+22S+5R+B+J+C"] == 73, pre_registered
        assert pre_registered["acceptance cap V_lane"] == 60, pre_registered
        assert pre_registered["acceptance cap V_general"] == 60, pre_registered
        assert pre_registered["acceptance cap V_lane"] <= pre_registered["acceptance cap V_general"]
        print(
            "C3-TWO-WORKER-PRE-REGISTRATION " + _json.dumps(pre_registered, sort_keys=True),
            flush=True,
        )

        runner = FlowRunner(entry.p2sat_app.get("c4_kill_recovery_flow"), pool, schema)
        flow_id = str((await runner.create_flow()).flow_id)

        # THE TWO OS WORKERS: both consume the flow's queues; the first
        # to assume leadership counts the lane's own R=1.
        for tag in ("c3-two-worker-a", "c3-two-worker-b"):
            pods.append(await _spawn_pod(conn, pg_dsn, schema, log_dir, tag))

        async def _done() -> bool:
            root = await pool.fetchval(
                f'SELECT status::text FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key='__flow__'",
                flow_id,
            )
            return root == "succeeded"

        await _wait_for(_done, 0.5, 300, "the two-worker lane never completed the run")

        # THE MEASUREMENT READS THE GUARANTEED FINAL SNAPSHOT (F5): the
        # pods are GRACEFULLY STOPPED (SIGTERM → the worker's own
        # termination → its `finally` writes the final census snapshot)
        # AND REAPED FIRST, and only then are the logs read. Reading the
        # periodic 10 s line while the pods are still running can
        # false-red a healthy lane (the run completing between ticks
        # leaves the last periodic line without the second admission); a
        # SIGKILL would prevent the finally from running at all — the
        # graceful stop is the measurement's own precondition.
        from tests.system_e2e._harness import graceful_stop as _graceful_stop

        for pod in pods:
            await asyncio.to_thread(_graceful_stop, pod)

        # THE MEASUREMENT: each pod's FINAL census snapshot (its last
        # line, written by the shutdown's own finally), summed — the
        # fleet's own operation instances. A follower that never ran an
        # operation has no census line and contributes zero (legal: the
        # leadership's own arms are the leader's; the R=1 term IS its
        # assumption's own count). THE MEASUREMENT MUST EXIST (N8): at
        # least one pod logged a census, the FINAL line exists for every
        # pod that logged at all, and it is LATER than (cumulatively at
        # least) every periodic line used as diagnostic history — a
        # vacuous zero-V pass is itself red.
        total: dict[str, int] = {}
        for pod in pods:
            pod_log = Path(pod.log_path)
            content = await asyncio.to_thread(pod_log.read_text)
            lines = [ln for ln in content.splitlines() if '"p2sat-census"' in ln]
            if not lines:
                continue
            docs = [_json.loads(ln) for ln in lines]
            snapshots = [
                {key: int(value) for key, value in doc.items() if type(value) is int}
                for doc in docs
            ]
            # THE GUARANTEED FINAL LINE (F5): the pod's own shutdown
            # `finally` writes its snapshot with `final=True` — the LAST
            # census line MUST be that final one (a suppressed final is
            # red: reading only a periodic line is the false measurement).
            assert docs[-1].get("final") is True, (
                "the pod's last census line is not its own final snapshot "
                f"(the shutdown's finally must have written it): {docs[-1]}"
            )
            final = snapshots[-1]
            # The final line is LATER than every periodic line: the
            # counts are cumulative, so the final dominates every
            # earlier snapshot in every family.
            for earlier in snapshots[:-1]:
                assert all(final.get(key, 0) >= value for key, value in earlier.items()), (
                    "the pod's final census snapshot must dominate every "
                    f"periodic line (the cumulative law): {earlier} -> {final}"
                )
            for key, value in final.items():
                if key != "final":
                    total[key] = total.get(key, 0) + value
        assert total, (
            "no pod logged its own census snapshot — the control must "
            "measure the fleet's own operation instances, never pass on "
            "an empty reading"
        )
        assert families_min_ok(total), total
        families = (
            total.get("admission_decisions", 0)
            + total.get("source_body_claims", 0)
            + total.get("fitted_grants", 0)
            + total.get("new_intents", 0)
            + total.get("flow_drains", 0)
            + total.get("global_drains", 0)
            + total.get("candidate_reads", 0)
        )
        print(
            "C3-TWO-WORKER-MEASURED V="
            + _json.dumps(
                {"V": families, "families": total, "pin": pre_registered},
                sort_keys=True,
            ),
            flush=True,
        )
        assert families <= pre_registered["acceptance cap V_lane"], (
            f"measured V {families} exceeds the two-worker lane cap "
            f"{pre_registered['acceptance cap V_lane']} (red): {total}"
        )
        # THE EVENT-SIDE LAW: E = T + X + K <= 2T + K.
        e = total.get("probes", 0) + total.get("observations", 0)
        assert e <= 2 * total.get("terminals", 0) + total.get("parks", 0), (
            f"E {e} > 2T + K — a hidden observation escaped the instrument"
        )
    finally:
        for pod in pods:
            await _stop(pod)
        await conn.close()
        await pool.close()
