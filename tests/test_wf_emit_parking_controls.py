# ruff: noqa: S608, ASYNC109
# Why: the schema is a fixture-derived test identifier and every value is
# $-bound; the recording proxies mirror asyncpg's own timeout-taking API; the
# bounded state helpers are deliberately small diagnostic polls, not production
# busy-waits.

"""Red-first controls for #718's slot-free emit backpressure parking.

These are the behavioral controls for the accepted P2 contract, not an
implementation of it.  They deliberately assert only application-visible row,
read, claim and retry facts:

* a saturated bounded source parks as ``status='scheduled'`` with the PARK
  INSTANT stored in ``scheduled_at`` (NOT NULL — the contract inverted the
  old NULL-timestamp representation; the marker, never the timestamp, is
  the discriminator) and the exact live
  ``metadata.blocking_reason='backpressure'`` marker (plus its versioned live
  record and durable deferral counter);
* the park refunds the physical ``jobs.attempt`` to zero, clears every
  lease/lock/heartbeat field, and leaves the current claim's ledger row
  ``awaited`` -- never ``failed``;
* the source's durable logical attempt and its public ``ctx.attempt`` stay at
  one across the physical ``1 -> 0 -> 1`` cycle;
* a direct ``FlowRunner.tick()`` does not treat the parked row as claimable
  work, does not run its body and does not reset/churn the parked row;
* the child terminal wakes the source exactly once, the source resumes from
  the persisted cursor, and the second page lands exactly once with no
  duplicate records;
* an unsaturated exact-fit control completes without a park; and
* a genuine source body failure still consumes the ordinary failed ledger /
  logical-attempt ladder and never acquires the backpressure marker.

On the current pre-P2 tree the saturated controls fail for the actual root
cause: the source body keeps its claim and polls ``EMIT_IN_FLIGHT_SQL`` every
50 ms, then the ordinary ladder burns a failed ledger terminal and advances the
source attempt.  The unsaturated and genuine-failure controls pass on that
tree, which keeps the file non-vacuous and proves the assertions are not merely
"the new API is absent".

The recording pool is a transparent delegate over a real asyncpg pool.  It
counts only actual executions of the rendered ``EMIT_IN_FLIGHT_SQL``; every
statement and transaction is still executed by the real connection.  A call
outside the known asyncpg connection surface raises instead of being silently
permissive.
"""

from __future__ import annotations

import asyncio
import enum
import inspect
import json
from collections.abc import Callable, Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    Chain,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows._emit import EMIT_CURSOR_KEY
from taskq.workflows._progress import ProgressRefusedError
from taskq.workflows.chain import DONE, Route
from tests._wf_fixtures import seed_join

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql


#: The largest source-admission read count the saturated two-page control will
#: tolerate: eight actual ``EMIT_IN_FLIGHT_SQL`` executions per page.  The
#: derivation is the accepted fixed cost, not a wall-clock budget: two reads
#: per admitted page (the pre-tx admission and the locked re-check) plus the
#: bounded post-park, direct-tick, child-terminal wake and resume probes.  The
#: base's 50 ms occupancy loop executes hundreds in the same window.
_MAX_ADMISSION_READS_PER_PAGE = 8

#: Bounded state helpers only: these are diagnostic polls over committed rows,
#: never the engine's own wait.  A saturated source is expected to park in one
#: admission decision on P2; the deadline merely lets the base execute its
#: default 30 s occupancy wait once so the red records the real defect.
_PARK_DEADLINE_S = 45.0
_WAKE_DEADLINE_S = 15.0
_FAILED_ROOT_OBSERVE_S = 5.0
_STATE_POLL_S = 0.02


class _RecordingPool:
    """A transparent asyncpg pool delegate that counts real admission reads."""

    def __init__(self, pool: asyncpg.Pool, emit_in_flight_sql: str) -> None:
        self._pool = pool
        self._emit_in_flight_sql = emit_in_flight_sql
        self.admission_reads = 0

    def acquire(self, *, timeout: float | None = None) -> _RecordingAcquire:
        return _RecordingAcquire(self, timeout=timeout)

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            "the admission-recording pool delegate saw an unexpected pool "
            f"attribute/operation {name!r}; extend the explicit delegate "
            "rather than making the fake permissive"
        )


class _RecordingAcquire:
    """The real pool's acquire context, yielding the recording connection."""

    def __init__(self, pool: _RecordingPool, *, timeout: float | None) -> None:
        self._pool = pool
        self._timeout = timeout
        self._connection: Any = None

    async def __aenter__(self) -> _RecordingConnection:
        self._connection = await self._pool._pool.acquire(timeout=self._timeout)
        return _RecordingConnection(self._pool, self._connection)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            await self._pool._pool.release(connection)


class _RecordingConnection:
    """A strict asyncpg connection delegate; only known calls are propagated."""

    def __init__(self, pool: _RecordingPool, connection: Any) -> None:
        self._pool = pool
        self._connection = connection

    async def execute(self, query: str, *args: object, timeout: float | None = None) -> str:
        return await self._connection.execute(query, *args, timeout=timeout)

    async def executemany(
        self,
        command: str,
        args: Iterable[Sequence[object]],
        *,
        timeout: float | None = None,
    ) -> str:
        return await self._connection.executemany(command, args, timeout=timeout)

    async def fetch(self, query: str, *args: object, timeout: float | None = None) -> list[Any]:
        return await self._connection.fetch(query, *args, timeout=timeout)

    async def fetchrow(self, query: str, *args: object, timeout: float | None = None) -> Any:
        return await self._connection.fetchrow(query, *args, timeout=timeout)

    async def fetchval(
        self,
        query: str,
        *args: object,
        column: int = 0,
        timeout: float | None = None,
    ) -> Any:
        if query == self._pool._emit_in_flight_sql:
            self._pool.admission_reads += 1
        return await self._connection.fetchval(query, *args, column=column, timeout=timeout)

    def transaction(
        self,
        *,
        isolation: str | None = None,
        readonly: bool = False,
        deferrable: bool = False,
    ) -> Any:
        return self._connection.transaction(
            isolation=isolation, readonly=readonly, deferrable=deferrable
        )

    async def add_listener(self, channel: str, callback: Callable[..., None]) -> None:
        await self._connection.add_listener(channel, callback)

    async def remove_listener(self, channel: str, callback: Callable[..., None]) -> None:
        await self._connection.remove_listener(channel, callback)

    def is_closed(self) -> bool:
        return bool(self._connection.is_closed())

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            "the admission-recording connection delegate saw an unexpected "
            f"asyncpg connection attribute/operation {name!r}; extend the "
            "explicit delegate rather than making the fake permissive"
        )


class _Outcome(enum.Enum):
    OK = "ok"


def _json_object(raw: object) -> dict[str, Any]:
    decoded: object = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise AssertionError(f"expected a JSON object, got {type(decoded).__name__}")
    return cast("dict[str, Any]", decoded)


def _park_is_live(state: dict[str, Any]) -> bool:
    # THE REPRESENTATION INVERSION (the contract changes the park's storage
    # face): the old shape rested a park at scheduled_at NULL; the new shape
    # stores the PARK INSTANT — scheduled_at is NOT NULL and deliberately
    # absent from park identity. The marker, never the timestamp,
    # discriminates, so liveness is the marker on a scheduled row.
    return (
        state["status"] == "scheduled"
        and state["scheduled_at"] is not None
        and state["blocking_reason"] == "backpressure"
    )


def _park_deferrals(metadata: dict[str, Any]) -> int:
    record = metadata.get("wf_emit_backpressure")
    if not isinstance(record, dict):
        raise AssertionError(
            "the live backpressure record metadata.wf_emit_backpressure is "
            f"absent: metadata={metadata!r}"
        )
    value = cast("dict[str, Any]", record).get("deferrals")
    if type(value) is not int:
        raise AssertionError(
            "metadata.wf_emit_backpressure.deferrals must be a JSON integer, "
            f"got {type(value).__name__} {value!r}"
        )
    return value


async def _source_state(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    source_id: JobId,
    source_key: str,
) -> dict[str, Any]:
    """Read the complete source row/ledger/root fact set for diagnostics."""
    row = await conn.fetchrow(
        f"SELECT status::text AS status, scheduled_at, attempt, claim_epoch, "
        "locked_by_worker, lock_expires_at, last_heartbeat_at, snooze_count, "
        f'error_class, error_message, error_traceback, metadata FROM "{schema}".jobs WHERE id = $1',
        source_id,
    )
    assert row is not None, f"the source row {source_id} disappeared"
    metadata = _json_object(row["metadata"])
    ledger = await conn.fetchrow(
        f'SELECT id, job_id, status::text AS status, attempt FROM "{schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = $2 "
        "ORDER BY attempt DESC, updated_at DESC LIMIT 1",
        flow_id,
        source_key,
    )
    failed_ledger = int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = $2 AND status = 'failed'",
            flow_id,
            source_key,
        )
    )
    root_status = await conn.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', flow_id
    )
    return {
        "status": str(row["status"]),
        "scheduled_at": row["scheduled_at"],
        "attempt": int(row["attempt"]),
        "claim_epoch": int(row["claim_epoch"]),
        "locked_by_worker": row["locked_by_worker"],
        "lock_expires_at": row["lock_expires_at"],
        "last_heartbeat_at": row["last_heartbeat_at"],
        "snooze_count": int(row["snooze_count"]),
        "error_class": row["error_class"],
        "error_message": row["error_message"],
        "error_traceback": row["error_traceback"],
        "metadata": metadata,
        "blocking_reason": metadata.get("blocking_reason"),
        "ledger_status": None if ledger is None else str(ledger["status"]),
        "ledger_attempt": None if ledger is None else int(ledger["attempt"]),
        "ledger_id": None if ledger is None else JobId(ledger["id"]),
        "ledger_job_id": None if ledger is None else JobId(ledger["job_id"]),
        "failed_ledger": failed_ledger,
        "root_status": None if root_status is None else str(root_status),
    }


async def _await_park_or_settled(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    source_id: JobId,
    source_key: str,
) -> dict[str, Any]:
    """Bounded diagnostic poll: parked, or the base has left the running state.

    On P2 the marker is committed by the park transaction before the source
    body exits.  On the pre-P2 base the same helper observes the source after
    the ordinary ladder has re-pended it (or times out loudly).  This poll is
    not the engine's wait and never substitutes for the source's own admission
    decision.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _PARK_DEADLINE_S
    state = await _source_state(conn, schema, flow_id, source_id, source_key)
    while loop.time() < deadline:
        state = await _source_state(conn, schema, flow_id, source_id, source_key)
        if _park_is_live(state):
            return state
        if state["status"] != "running":
            return state
        await asyncio.sleep(_STATE_POLL_S)
    state["diagnostic_timeout"] = True
    return state


async def _await_source_status(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    source_id: JobId,
    source_key: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    deadline_s: float,
) -> dict[str, Any]:
    """Bounded diagnostic state wait (never an engine sleep)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_s
    state = await _source_state(conn, schema, flow_id, source_id, source_key)
    while loop.time() < deadline:
        state = await _source_state(conn, schema, flow_id, source_id, source_key)
        if predicate(state):
            return state
        await asyncio.sleep(_STATE_POLL_S)
    state["diagnostic_timeout"] = True
    return state


async def _children(
    conn: asyncpg.Connection, schema: str, source_id: JobId, step_key: str
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"SELECT map_index, trace_id, idempotency_key, status::text AS status "
        f'FROM "{schema}".jobs WHERE parent_id = $1 AND step_key = $2 '
        "ORDER BY map_index",
        source_id,
        step_key,
    )


async def _cursor(conn: asyncpg.Connection, schema: str, source_id: JobId) -> object:
    raw = await conn.fetchval(
        f'SELECT metadata->$2 FROM "{schema}".jobs WHERE id = $1',
        source_id,
        EMIT_CURSOR_KEY,
    )
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


def _source_body(
    chain: Chain,
    pages: Sequence[Sequence[int]],
    observed: list[tuple[int, int]],
    *,
    page_zero_emitted: asyncio.Event | None = None,
    wait_before_page_one: asyncio.Event | None = None,
) -> Any:
    """A real paged source body over deterministic synthetic record ids."""

    async def source(ctx: StepContext) -> None:
        cursor = await ctx.cursor()
        raw_page = cursor.get("page", -1)
        start_page = int(cast("int", raw_page)) + 1
        for page in range(start_page, len(pages)):
            observed.append((page, int(ctx.attempt)))
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"record_id": record_id},
                        map_index=record_id,
                        trace_id=f"record-{record_id}",
                    )
                    for record_id in pages[page]
                ],
                cursor={"page": page},
            )
            if page == 0 and page_zero_emitted is not None:
                page_zero_emitted.set()
            if page == 0 and wait_before_page_one is not None:
                await wait_before_page_one.wait()

    return source


def _chain(name: str, screen: Any) -> Chain:
    return Chain(
        name=name,
        start="screen",
        steps={"screen": Step(body=screen, outcomes=_Outcome, route=Route({_Outcome.OK: DONE}))},
    )


def _declaration_facts(compiled: Any, bound: int) -> dict[str, object]:
    """The bound stays the known workflow declaration; max_pending is separate."""
    return {
        "compiled_bound": compiled.max_in_flight,
        "compiled_max_pending": getattr(compiled, "max_pending", None),
        "workflow_accepts_max_in_flight": (
            "max_in_flight" in inspect.signature(WorkflowApp.workflow).parameters
        ),
        "chain_source_has_no_max_in_flight": (
            "max_in_flight" not in inspect.signature(chain_source).parameters
        ),
        "bound": bound,
    }


def _assert_declaration_facts(compiled: Any, bound: int) -> None:
    facts = _declaration_facts(compiled, bound)
    assert facts == {
        "compiled_bound": bound,
        "compiled_max_pending": None,
        "workflow_accepts_max_in_flight": True,
        "chain_source_has_no_max_in_flight": True,
        "bound": bound,
    }, (
        "the declared emit bound must stay the existing workflow-level "
        f"max_in_flight ({bound}) with no max_pending reuse: {facts!r}"
    )


# ── 1. The saturated source parks, releases, resumes and terminates ───────
#    This also carries the direct-driver parked-marker control: while the
#    source is parked, an explicit tick() must perform no claim/body work and
#    must not reset the parked row; the subsequent drive() must reach terminal
#    rather than max_ticks.


@pytest.mark.integration
async def test_saturated_source_parks_without_retry_and_resumes_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    suffix = new_uuid().hex[:12]
    bound = 2
    pages = ((0, 1), (2, 3))
    gate = asyncio.Event()
    child_zero_started = asyncio.Event()
    child_one_started = asyncio.Event()
    page_zero_emitted = asyncio.Event()
    source_attempts: list[tuple[int, int]] = []
    child_records: list[int | None] = []

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        child_records.append(ctx.map_index)
        if ctx.map_index == 0:
            child_zero_started.set()
        elif ctx.map_index == 1:
            child_one_started.set()
        await gate.wait()
        return _Outcome.OK

    chain = _chain(f"parking-chain-{suffix}", screen)
    source_body = _source_body(chain, pages, source_attempts, page_zero_emitted=page_zero_emitted)
    app = WorkflowApp()

    @app.workflow(f"parking_flow_{suffix}", max_in_flight=bound)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=4))

    compiled = app.get(f"parking_flow_{suffix}")
    recording_pool = _RecordingPool(wf_pool, wf_sql.emit_capacity_observation)
    source_runner = FlowRunner(compiled, cast("asyncpg.Pool", recording_pool), wf_schema)
    child_runner_zero = FlowRunner(compiled, cast("asyncpg.Pool", recording_pool), wf_schema)
    child_runner_one = FlowRunner(compiled, cast("asyncpg.Pool", recording_pool), wf_schema)
    claim = await source_runner.create_flow()
    flow_id = claim.flow_id
    source_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert source_row is not None, "the created flow must carry its source node"
    source_id = JobId(source_row["id"])

    source_task = asyncio.create_task(source_runner.tick(flow_id))
    child_tasks: list[asyncio.Task[bool]] = []
    parked: dict[str, Any] | None = None
    at_park_attempts: list[tuple[int, int]] | None = None
    direct_tick_ran: bool | None = None
    direct_tick_state: dict[str, Any] | None = None
    direct_tick_attempts: list[tuple[int, int]] | None = None
    wake_state: dict[str, Any] | None = None
    drive_result: str | None = None
    failure: BaseException | None = None
    try:
        await asyncio.wait_for(page_zero_emitted.wait(), timeout=_PARK_DEADLINE_S)
        child_tasks.append(asyncio.create_task(child_runner_zero.tick(flow_id)))
        await asyncio.wait_for(child_zero_started.wait(), timeout=_WAKE_DEADLINE_S)
        child_tasks.append(asyncio.create_task(child_runner_one.tick(flow_id)))
        await asyncio.wait_for(child_one_started.wait(), timeout=_WAKE_DEADLINE_S)
        observed = await _await_park_or_settled(wf_conn, wf_schema, flow_id, source_id, "source")
        if _park_is_live(observed):
            parked = observed
            await asyncio.wait_for(source_task, timeout=_WAKE_DEADLINE_S)
            # The body records (page, attempt) before emit_batch parks, so the
            # parked page-1 attempt is already in the list here; snapshot it
            # as the exact expected no-op state for the direct tick below.
            at_park_attempts = list(source_attempts)
            direct_tick_ran = await source_runner.tick(flow_id)
            direct_tick_state = await _source_state(
                wf_conn, wf_schema, flow_id, source_id, "source"
            )
            direct_tick_attempts = list(source_attempts)
        gate.set()
        await asyncio.wait_for(asyncio.gather(*child_tasks), timeout=_WAKE_DEADLINE_S)
        if parked is not None:
            wake_state = await _await_source_status(
                wf_conn,
                wf_schema,
                flow_id,
                source_id,
                "source",
                lambda state: state["status"] == "pending",
                deadline_s=_WAKE_DEADLINE_S,
            )
        drive_result = await source_runner.drive(flow_id, max_ticks=250)
    except BaseException as exc:
        failure = exc
    finally:
        gate.set()
        if not source_task.done():
            source_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await source_task
        for child_task in child_tasks:
            if not child_task.done():
                child_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await child_task

    if failure is not None:
        raise failure

    final_source = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    final_children = await _children(wf_conn, wf_schema, source_id, "screen")
    final_cursor = await _cursor(wf_conn, wf_schema, source_id)
    final_root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    observed = {
        "parked": parked,
        "direct_tick_ran": direct_tick_ran,
        "direct_tick_state": direct_tick_state,
        "direct_tick_attempts": direct_tick_attempts,
        "wake_state": wake_state,
        "drive_result": drive_result,
        "source_attempts": source_attempts,
        "final_source": final_source,
        "final_children": [dict(row) for row in final_children],
        "final_cursor": final_cursor,
        "final_root": final_root,
        "admission_reads": recording_pool.admission_reads,
        "max_admission_reads": _MAX_ADMISSION_READS_PER_PAGE * len(pages),
        "child_records": child_records,
    }
    diagnostic = json.dumps(observed, default=str, sort_keys=True)
    assert parked is not None, (
        "the source under the saturated bound did not park with the exact "
        "live marker (status scheduled + the park instant in scheduled_at + "
        "blocking_reason='backpressure'); on the base it stayed claimed and "
        "the 50 ms occupancy loop handed the page to the retry ladder: "
        f"{diagnostic}"
    )
    assert parked["blocking_reason"] == "backpressure", diagnostic
    # REPRESENTATION INVERSION: the park instant is now stored in
    # scheduled_at (NOT NULL — never a NULL due date and never a next fire).
    assert parked["scheduled_at"] is not None, diagnostic
    assert parked["attempt"] == 0, (
        f"the park must refund the physical claim attempt to zero at rest: {diagnostic}"
    )
    assert parked["locked_by_worker"] is None, diagnostic
    assert parked["lock_expires_at"] is None, diagnostic
    assert parked["last_heartbeat_at"] is None, diagnostic
    assert parked["metadata"].get("wf_deferral", {}).get("signal") == "EmitBackpressure", (
        f"the live metadata must carry the typed EmitBackpressure deferral signal: {diagnostic}"
    )
    assert _park_deferrals(parked["metadata"]) == 1, (
        f"one park must record exactly one durable backpressure deferral: {diagnostic}"
    )
    assert parked["snooze_count"] == 0, (
        f"healthy materialization backpressure must not move generic snooze_count: {diagnostic}"
    )
    assert parked["metadata"].get("wf_source_logical_attempt") == 1, (
        f"the source's durable logical attempt must stay 1 while parked: {diagnostic}"
    )
    assert parked["ledger_status"] == "awaited", (
        f"the park must mark the current claim's ledger row awaited, never failed: {diagnostic}"
    )
    assert parked["ledger_attempt"] == 1, diagnostic
    assert isinstance(parked["ledger_id"], UUID), diagnostic
    assert parked["ledger_job_id"] == source_id, diagnostic
    assert parked["failed_ledger"] == 0, (
        f"saturation must write zero failed source ledger terminals: {diagnostic}"
    )
    assert direct_tick_ran is False, (
        "an explicit FlowRunner.tick() must not treat the parked marker row as "
        f"claimable work or run its body: {diagnostic}"
    )
    assert direct_tick_state is not None, diagnostic
    assert direct_tick_state["status"] == "scheduled", diagnostic
    assert direct_tick_state["scheduled_at"] is not None, diagnostic
    assert direct_tick_state["attempt"] == parked["attempt"], diagnostic
    assert direct_tick_state["claim_epoch"] == parked["claim_epoch"], diagnostic
    assert direct_tick_state["blocking_reason"] == "backpressure", diagnostic
    assert _park_deferrals(direct_tick_state["metadata"]) == 1, diagnostic
    assert direct_tick_state["failed_ledger"] == 0, diagnostic
    assert direct_tick_attempts == at_park_attempts == [(0, 1), (1, 1)], (
        f"the direct tick while parked must not re-enter the source body: {diagnostic}"
    )
    assert source_attempts == [(0, 1), (1, 1), (1, 1)], (
        "the source must record exactly page 0, the parked page-1 attempt, "
        "and the single post-wake page-1 re-entry from the persisted cursor "
        f"(no logical/physical retry consumption): {diagnostic}"
    )
    assert {attempt for _, attempt in source_attempts} == {1}, (
        "every source body entry must observe logical ctx.attempt == 1 across "
        f"the physical 1 -> 0 -> 1 refund cycle: {diagnostic}"
    )
    assert drive_result == "terminal", (
        "after the child terminal wakes the parked source, drive() must reach "
        f"terminal rather than max_ticks: {diagnostic}"
    )
    assert wake_state is not None and wake_state["status"] == "pending", (
        "the fitting child terminal must promote the parked source to pending "
        f"and remove the live marker: {diagnostic}"
    )
    assert wake_state["blocking_reason"] is None, (
        "promotion must remove the live blocking_reason marker exactly at the "
        "wake transition: a newly promoted pending source must not stay "
        "resolved-marked nor coexist with a new park marker/due stamp: "
        f"{diagnostic}"
    )
    assert final_root == "succeeded", f"the run must complete normally: {diagnostic}"
    assert final_source["status"] == "succeeded", diagnostic
    assert final_source["blocking_reason"] is None, (
        f"the woken source's final row must not carry the live park marker: {diagnostic}"
    )
    assert final_source["attempt"] == 1, (
        "the resumed source's physical attempt must be the reminted logical "
        f"attempt 1, never an inflated ladder attempt: {diagnostic}"
    )
    # THE DIRECT DOOR'S OWN CLAIM SEQUENCE (the epoch arbiter's law): the
    # resumed claim granted the row's NEXT epoch (1 → 2), the parked
    # claim's 'awaited' row survives beside the resumed claim's terminal,
    # and the latest row is the resumed success.
    final_history = await wf_conn.fetch(
        f'SELECT status::text AS status, attempt, claim_epoch FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'source' "
        "ORDER BY attempt, claim_epoch, created_at, id",
        flow_id,
    )
    assert [(r["status"], r["attempt"], r["claim_epoch"]) for r in final_history] == [
        ("awaited", 1, 1),
        ("succeeded", 1, 2),
    ], [tuple(r) for r in final_history]
    assert final_source["failed_ledger"] == 0, diagnostic
    assert final_source["snooze_count"] == 0, diagnostic
    assert _park_deferrals(final_source["metadata"]) == 1, (
        f"the durable deferral counter must remain exactly one after the wake: {diagnostic}"
    )
    assert final_cursor == {"page": 1}, (
        f"the cursor must equal the last committed page exactly once: {diagnostic}"
    )
    assert [int(row["map_index"]) for row in final_children] == [0, 1, 2, 3], (
        f"the second page must contain exactly records 2 and 3 and no duplicate: {diagnostic}"
    )
    assert all(row["status"] == "succeeded" for row in final_children), diagnostic
    assert len({row["idempotency_key"] for row in final_children}) == 4, (
        f"the four emitted records must keep four distinct idempotency keys: {diagnostic}"
    )
    assert sorted(child_records) == [0, 1, 2, 3], (
        f"all four child bodies must have run exactly once: {diagnostic}"
    )
    assert recording_pool.admission_reads <= _MAX_ADMISSION_READS_PER_PAGE * len(pages), (
        "healthy parking must keep source admission reads O(pages), not O(elapsed "
        f"time): observed {recording_pool.admission_reads} for {len(pages)} pages: "
        f"{diagnostic}"
    )
    _assert_declaration_facts(compiled, bound)


# ── 2. A failed root leaves the parked source resolved and inert ──────────


@pytest.mark.integration
async def test_failed_root_keeps_parked_source_resolved_and_inert(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    suffix = new_uuid().hex[:12]
    bound = 3
    pages = ((0, 1), (2, 3, 4))
    page_zero_emitted = asyncio.Event()
    allow_page_one = asyncio.Event()
    source_attempts: list[tuple[int, int]] = []
    child_calls: list[int | None] = []

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        child_calls.append(ctx.map_index)
        if ctx.map_index == 0:
            raise ProgressRefusedError("synthetic deterministic child failure")
        return _Outcome.OK

    chain = _chain(f"parking-failed-root-chain-{suffix}", screen)
    source_body = _source_body(
        chain,
        pages,
        source_attempts,
        page_zero_emitted=page_zero_emitted,
        wait_before_page_one=allow_page_one,
    )
    app = WorkflowApp()

    @app.workflow(f"parking_failed_root_{suffix}", max_in_flight=bound)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=4))

    compiled = app.get(f"parking_failed_root_{suffix}")
    source_runner = FlowRunner(compiled, wf_pool, wf_schema)
    child_runner = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await source_runner.create_flow()
    flow_id = claim.flow_id
    source_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert source_row is not None, "the created flow must carry its source node"
    source_id = JobId(source_row["id"])

    source_task = asyncio.create_task(source_runner.tick(flow_id))
    child_task: asyncio.Task[bool] | None = None
    parked: dict[str, Any] | None = None
    failure: BaseException | None = None
    try:
        await asyncio.wait_for(page_zero_emitted.wait(), timeout=_PARK_DEADLINE_S)
        # A resolved-blocked join waiter occupies one unit of materialization
        # capacity while the root's failure derivation still treats it as
        # RESOLVED (metadata.blocking_reason='join').  That is the real
        # failed-root-with-live-page case: page 1 still cannot fit after both
        # children terminalize, so the source must stay parked and inert.
        join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
        child_task = asyncio.create_task(child_runner.tick(flow_id))
        await asyncio.wait_for(child_task, timeout=_WAKE_DEADLINE_S)
        allow_page_one.set()
        # The wait accepts the LIVE park or the §5.4 fence that superseded
        # it (the child's exhausted failure roots-fail fast; the fence is
        # the same transaction's outcome).
        parked = await _await_source_status(
            wf_conn,
            wf_schema,
            flow_id,
            source_id,
            "source",
            lambda state: (
                _park_is_live(state)
                or (
                    state["status"] == "failed"
                    and state["error_class"] == "WorkflowNotDrainingError"
                )
            ),
            deadline_s=_FAILED_ROOT_OBSERVE_S,
        )
        if not (
            _park_is_live(parked)
            or (
                parked["status"] == "failed" and parked["error_class"] == "WorkflowNotDrainingError"
            )
        ):
            children_now = await _children(wf_conn, wf_schema, source_id, "screen")
            cursor_now = await _cursor(wf_conn, wf_schema, source_id)
            root_now = await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
            )
            raise AssertionError(
                "the saturated source did not park before the failed-root "
                "control could observe it: "
                + json.dumps(
                    {
                        "source": parked,
                        "children": [dict(row) for row in children_now],
                        "cursor": cursor_now,
                        "root": root_now,
                        "source_attempts": source_attempts,
                        "child_calls": child_calls,
                    },
                    default=str,
                    sort_keys=True,
                )
            )
        await asyncio.wait_for(source_task, timeout=_WAKE_DEADLINE_S)
        # The park itself does not run the root-maintenance leg; a direct tick
        # must mirror the fleet sweep and derive the failed root while the
        # source is parked.
        await source_runner.tick(flow_id)
        root_failed = await _await_root_status(
            wf_conn, wf_schema, flow_id, "failed", deadline_s=_WAKE_DEADLINE_S
        )
        snapshot = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
        children_now = await _children(wf_conn, wf_schema, source_id, "screen")
        cursor_now = await _cursor(wf_conn, wf_schema, source_id)
        root_now = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        root_diagnostic = json.dumps(
            {
                "source": snapshot,
                "children": [dict(row) for row in children_now],
                "cursor": cursor_now,
                "root": root_now,
                "source_attempts": source_attempts,
                "child_calls": child_calls,
            },
            default=str,
            sort_keys=True,
        )
        assert root_failed, (
            "the failed child must terminalize the root while the source is "
            f"parked: {root_diagnostic}"
        )
        # BEHAVIOR SUPERSESSION (the contract's §5.4): the failed root
        # behind this child's EXHAUSTED retry policy, with the source's
        # exact park still holding un-emitted work (including the first
        # page when no cursor has committed), is the proven non-drainage
        # shape — the SAME failure transaction fenced the
        # source (the old law left it live-parked resolved; that law is
        # superseded). The fence is the refusal: the named durable class,
        # the reason, the causal ids, no ledger 'failed' ladder charge.
        assert snapshot["status"] == "failed", (
            f"the proven non-drainage fence terminalizes the parked source: {root_diagnostic}"
        )
        assert snapshot["error_class"] == "WorkflowNotDrainingError", root_diagnostic
        assert snapshot["failed_ledger"] == 0, (
            "the fence spends no source ladder charge: the refusal is not a "
            f"failed ladder terminal on the source's own claim: {root_diagnostic}"
        )
        # Remove the resolved-blocked waiter so a spurious wake would have
        # something to win. The FENCED source must never re-arm: pumping
        # the direct driver must not create a later claim, page or cursor
        # movement.
        await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', join_id)
        for _ in range(3):
            await source_runner.tick(flow_id)
        inert = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
        children = await _children(wf_conn, wf_schema, source_id, "screen")
        cursor = await _cursor(wf_conn, wf_schema, source_id)
        inert_root = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        inert_diagnostic = json.dumps(
            {
                "source": inert,
                "children": [dict(row) for row in children],
                "cursor": cursor,
                "root": inert_root,
                "source_attempts": source_attempts,
                "child_calls": child_calls,
            },
            default=str,
            sort_keys=True,
        )
        assert inert["status"] == "failed", (
            f"the fenced source stays terminal (a failed root never re-arms it): {inert_diagnostic}"
        )
        assert inert["error_class"] == "WorkflowNotDrainingError", inert_diagnostic
        assert inert["attempt"] == snapshot["attempt"] == 0, inert_diagnostic
        assert source_attempts == [(0, 1), (1, 1)], (
            "the failed root must never re-enter the parked source's body: "
            "exactly the page-0 entry and the parked page-1 attempt may exist, "
            f"with no later entry after the root fails: {inert_diagnostic}"
        )
        assert [int(row["map_index"]) for row in children] == [0, 1], (
            f"no later page children may be materialized under the failed root: {inert_diagnostic}"
        )
        assert child_calls == [0, 1], (
            "both real page-0 child bodies must have run before the root "
            f"failure derived: {inert_diagnostic}"
        )
        assert cursor == {"page": 0}, (
            f"the failed-root source's cursor must stay at its last committed page: {inert_diagnostic}"
        )
    except BaseException as exc:
        failure = exc
    finally:
        allow_page_one.set()
        if not source_task.done():
            source_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await source_task
        if child_task is not None and not child_task.done():
            child_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await child_task

    if failure is not None:
        raise failure


async def _await_root_status(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    expected: str,
    *,
    deadline_s: float,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_s
    while loop.time() < deadline:
        status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', flow_id
        )
        if status == expected:
            return True
        await asyncio.sleep(_STATE_POLL_S)
    return False


# ── 3. Unsaturated exact-fit control: the bound still admits whole pages ──


@pytest.mark.integration
async def test_unsaturated_exact_fit_pages_complete_without_parking(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    suffix = new_uuid().hex[:12]
    bound = 4
    pages = ((0, 1), (2, 3))
    source_attempts: list[tuple[int, int]] = []

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        return _Outcome.OK

    chain = _chain(f"parking-exact-fit-chain-{suffix}", screen)
    source_body = _source_body(chain, pages, source_attempts)
    app = WorkflowApp()

    @app.workflow(f"parking_exact_fit_{suffix}", max_in_flight=bound)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=4))

    compiled = app.get(f"parking_exact_fit_{suffix}")
    recording_pool = _RecordingPool(wf_pool, wf_sql.emit_capacity_observation)
    runner = FlowRunner(compiled, cast("asyncpg.Pool", recording_pool), wf_schema)
    claim = await runner.create_flow()
    flow_id = claim.flow_id
    source_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert source_row is not None, "the created flow must carry its source node"
    source_id = JobId(source_row["id"])

    assert await runner.drive(flow_id) == "terminal", (
        "the unsaturated exact-fit run must reach terminal"
    )
    state = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    children = await _children(wf_conn, wf_schema, source_id, "screen")
    cursor = await _cursor(wf_conn, wf_schema, source_id)
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    observed = {
        "source": state,
        "children": [dict(row) for row in children],
        "cursor": cursor,
        "root": root,
        "source_attempts": source_attempts,
        "admission_reads": recording_pool.admission_reads,
    }
    diagnostic = json.dumps(observed, default=str, sort_keys=True)
    assert state["blocking_reason"] is None, diagnostic
    assert state["status"] == "succeeded", diagnostic
    assert state["attempt"] == 1, diagnostic
    assert state["failed_ledger"] == 0, diagnostic
    assert state["snooze_count"] == 0, diagnostic
    assert [page for page, _ in source_attempts] == [0, 1], diagnostic
    assert {attempt for _, attempt in source_attempts} == {1}, diagnostic
    assert [int(row["map_index"]) for row in children] == [0, 1, 2, 3], diagnostic
    assert all(row["status"] == "succeeded" for row in children), diagnostic
    assert len({row["idempotency_key"] for row in children}) == 4, diagnostic
    assert cursor == {"page": 1}, diagnostic
    assert root == "succeeded", diagnostic
    assert recording_pool.admission_reads <= _MAX_ADMISSION_READS_PER_PAGE * len(pages), (
        f"the unsaturated exact-fit control must keep admission reads O(pages): {diagnostic}"
    )
    _assert_declaration_facts(compiled, bound)


# ── 4. Genuine source failure stays a failure, never a park ───────────────


@pytest.mark.integration
async def test_genuine_source_failure_still_consumes_the_failure_ladder(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    suffix = new_uuid().hex[:12]
    source_attempts: list[int] = []

    async def screen(ctx: StepContext, item: object) -> _Outcome:
        return _Outcome.OK

    async def failing_source(ctx: StepContext) -> None:
        source_attempts.append(int(ctx.attempt))
        raise RuntimeError("synthetic source body transport failure")

    chain = _chain(f"parking-failure-chain-{suffix}", screen)
    app = WorkflowApp()

    @app.workflow(f"parking_failure_{suffix}", max_in_flight=4)
    def flow() -> Promise[object]:
        return build(chain_source(chain, failing_source, key="source", max_attempts=2))

    compiled = app.get(f"parking_failure_{suffix}")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await runner.create_flow()
    flow_id = claim.flow_id
    source_row = await wf_conn.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
        flow_id,
    )
    assert source_row is not None, "the created flow must carry its source node"
    source_id = JobId(source_row["id"])

    assert await runner.drive(flow_id) == "terminal", (
        "a genuinely failing source must still reach the declared terminal"
    )
    state = await _source_state(wf_conn, wf_schema, flow_id, source_id, "source")
    children = await _children(wf_conn, wf_schema, source_id, "screen")
    cursor = await _cursor(wf_conn, wf_schema, source_id)
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    observed = {
        "source": state,
        "children": [dict(row) for row in children],
        "cursor": cursor,
        "root": root,
        "source_attempts": source_attempts,
    }
    diagnostic = json.dumps(observed, default=str, sort_keys=True)
    assert root == "failed", diagnostic
    assert state["status"] == "failed", diagnostic
    assert state["blocking_reason"] is None, (
        f"a genuine source failure must never acquire the live backpressure marker: {diagnostic}"
    )
    assert "wf_emit_backpressure" not in state["metadata"], diagnostic
    assert state["scheduled_at"] is not None, (
        f"the failed source is a real terminal, not a parked row: {diagnostic}"
    )
    assert state["failed_ledger"] == 2, (
        f"both genuine failed attempts must remain failed ledger terminals: {diagnostic}"
    )
    assert source_attempts == [1, 2], (
        "the public logical attempts must advance exactly with the genuine "
        f"body failures: {diagnostic}"
    )
    assert state["metadata"].get("wf_source_logical_attempt") == 3, (
        "two genuine failures must advance the durable logical carrier to 3, "
        f"never be refunded as backpressure: {diagnostic}"
    )
    assert state["snooze_count"] == 0, f"generic snooze_count must stay untouched: {diagnostic}"
    assert children == [], "a failing source before any emit must write no page"
    assert cursor is None, diagnostic
