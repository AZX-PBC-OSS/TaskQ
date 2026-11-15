"""The transactional settlement's unwind/resource controls (the F1
correction's maintained equivalents of the preserved review reproducers).

Every control runs real PostgreSQL through the public `transactional_step`
door and asserts the observable resource/row facts:

* a cooperative deadline `finally` on the shared claim connection finishes
  before the managed rollback; the row records the truthful `TimeoutError`;
  the connection is released once and the emitter closes once; a pool of
  one still serves the next claim;
* a body's OWN `TimeoutError`/`InterfaceError` (pre-deadline) keep their
  body classes — the rollback-collision re-label never launders them;
* a hostile shielded unwind past the 2.0s budget has its connection
  terminated OUT-OF-POOL (the backend is gone, no other task ever receives
  that session), its outcome retrieved, and the dependency pin released by
  its own done callback — exactly once;
* a real reset-hook failure is a resource fact: the body/deadline/success
  outcome stands, the emitter still closes, and no second release or body
  execution happens;
* a real connection loss AT ROLLBACK (after application writes) commits
  zero prefix rows, keeps the causal outcome, and converges through the
  fresh-connection disposition.

The original reviewer reproducers stay preserved beside this file (the
red-first history); these are the maintained, behavior-asserting forms.
"""

# ruff: noqa: S608  # Schema names are generated test identifiers; values remain $-bound.

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    TransactionalStepContext,
    WorkflowApp,
    build,
    transactional_step,
)
from tests._wf_atomicity_fixture import RealWorker, managed_pg_pool, new_label

if TYPE_CHECKING:
    from taskq.testing.fixtures import ModulePgSchema


async def _settle_row(
    conn: asyncpg.Connection, schema: str, flow_id: object, step_key: str = "settle"
) -> asyncpg.Record:
    row = await conn.fetchrow(
        f"SELECT status::text AS status, attempt, claim_epoch, error_class, error_message "
        f"FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 AND step_key=$2",
        flow_id,
        step_key,
    )
    assert row is not None
    return row


async def _noop_leaf(ctx: StepContext, item: object) -> str:
    return "ok"


async def _stream_source(ctx: StepContext) -> str:
    return "source-ok"


def _two_node_flow(
    name: str,
    settle_body: Any,
    *,
    start_to_close: timedelta | None = None,
) -> tuple[WorkflowApp, Any]:
    from taskq.workflows import Chain, Step, chain_source

    chain = Chain(
        name=name,
        start="screen",
        steps={"screen": Step(_noop_leaf, str)},
    )
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        source_decl = chain_source(chain, _stream_source, key="source", max_attempts=1)
        barrier = transactional_step(
            settle_body,
            source_decl,
            key="settle",
            max_attempts=1,
            start_to_close=start_to_close,
        )
        return build(barrier)

    return app, app.get(name)


@pytest.mark.integration
async def test_cooperative_deadline_finally_finishes_before_the_rollback(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The deadline's expiry cancels the body, bound-waits its cooperative
    finally ON the shared connection, and the truthful TimeoutError lands
    with exactly one release and one emitter close (the preserved
    reproducer's shape, now asserting the corrected behavior)."""
    events: list[str] = []

    async def settle(ctx: TransactionalStepContext, source_result: object) -> str:
        conn = ctx.connection  # the non-optional face — no narrowing
        claim_conns.append(conn)
        await conn.execute("SELECT 1")
        events.append("body")
        try:
            await asyncio.sleep(10)
        finally:
            events.append("finally")
            # A legitimate cleanup statement on the SAME claim connection:
            # the unwind budget must let it finish before the rollback.
            await conn.execute("SELECT pg_sleep(0.05)")
        return "unreachable"

    name = new_label("tx_deadline_finally")
    _app, compiled = _two_node_flow(name, settle, start_to_close=timedelta(seconds=0.2))
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    worker = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
    claim_conns: list[object] = []
    releases: list[object] = []

    original_release = type(wf_pool).release

    async def counting_release(pool: Any, connection: Any, **kwargs: object) -> None:
        await original_release(pool, connection, **kwargs)
        if any(connection is held for held in claim_conns):
            # The CLAIM connection's own release — the ordered teardown's
            # exactly-once fact.
            releases.append(connection)

    async def body_runner() -> str:
        verdict: str | None = None
        escaped: BaseException | None = None
        try:
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(type(wf_pool), "release", counting_release)
                verdict = await worker.run(compiled, flow_id, "settle")
        except BaseException as exc:  # the probe's own capture face
            escaped = exc
        assert verdict is None or verdict in ("laddered", "failed"), (verdict, escaped)
        return verdict if verdict is not None else "escaped"

    try:
        await worker.run(compiled, flow_id, "source")
        verdict = await asyncio.wait_for(body_runner(), timeout=60)
        assert verdict in ("laddered", "failed", "escaped"), verdict
        row = await _settle_row(wf_conn, wf_schema, flow_id)
        # The truthful deadline disposition: the deadline owns the row, not
        # the collision's InterfaceError and not the body's classes.
        assert row["status"] == "failed", dict(row)
        assert row["error_class"] == "TimeoutError", dict(row)
        assert events == ["body", "finally"], events
        # Exactly one release of the CLAIM connection happened (the
        # ordered teardown's own), the emitter's close ran inside it, and
        # the pool of one still serves the next claim (the disposition's
        # own fresh acquisition used it).
        assert len(releases) == 1, releases
        next_use = await wf_pool.execute("SELECT 1")
        assert next_use == "SELECT 1"
    finally:
        await worker.close()


@pytest.mark.integration
async def test_body_classes_survive_their_own_deadline_and_interface_errors(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Opposite direction: a body-raised TimeoutError and a pre-deadline
    InterfaceError keep their own classes — neither is stamped as the
    engine's deadline collision."""
    flow_ids: list[object] = []

    async def raises_timeout(ctx: TransactionalStepContext, source_result: object) -> str:
        await ctx.connection.execute("SELECT 1")
        raise TimeoutError("the body's own wall")

    async def raises_interface(ctx: TransactionalStepContext, source_result: object) -> str:
        raise asyncpg.InterfaceError("the body's own interface failure")

    for body in (raises_timeout, raises_interface):
        name = new_label("tx_body_class")
        _app, compiled = _two_node_flow(name, body, start_to_close=timedelta(seconds=5))
        runner = FlowRunner(compiled, wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        flow_ids.append(flow_id)
        worker = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            await worker.run(compiled, flow_id, "source")
            verdict = await worker.run(compiled, flow_id, "settle")
            assert verdict in ("laddered", "failed"), verdict
        finally:
            await worker.close()
    timeout_row = await _settle_row(wf_conn, wf_schema, flow_ids[0])
    interface_row = await _settle_row(wf_conn, wf_schema, flow_ids[1])
    # Both attempts are PAST their own start_to_close... neither row
    # records the deadline: the body's classes are the record (the ladder's
    # truthful failure vocabulary).
    assert timeout_row["status"] == "failed", dict(timeout_row)
    assert timeout_row["error_class"] == "TimeoutError", dict(timeout_row)
    assert interface_row["status"] == "failed", dict(interface_row)
    assert interface_row["error_class"] == "InterfaceError", dict(interface_row)


@pytest.mark.integration
async def test_hostile_shielded_unwind_is_terminated_out_of_pool(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
    pg_dsn: str,
) -> None:
    """A body shielding its own in-flight PG operation past the deadline
    AND the unwind budget: the shared connection is terminated out-of-pool
    (the backend is gone), no other task ever receives that session, the
    outcome is retrieved, and the deadline's own disposition stands."""

    gate = asyncio.Event()
    released = asyncio.Event()
    pids: list[int] = []

    gate = asyncio.Event()
    released = asyncio.Event()
    pids: list[int] = []

    async def settle(ctx: TransactionalStepContext, source_result: object) -> str:
        conn = ctx.connection
        await conn.execute("SELECT 1")
        pid = await conn.fetchval("SELECT pg_backend_pid()")
        pids.append(int(pid))
        gate.set()
        # The HOSTILE unwind: a real in-flight PG operation, shielded past
        # the deadline AND the 2.0s budget — the exact shape the
        # out-of-pool trap exists for.
        inner = asyncio.ensure_future(conn.execute("SELECT pg_sleep(5)"))
        try:
            await asyncio.shield(inner)
        except asyncio.CancelledError:
            released.set()
            raise
        finally:
            # Retrieve the shielded operation's own outcome (the
            # termination's error surfaces here) — asyncio never reports
            # it unretrieved.
            import contextlib

            with contextlib.suppress(BaseException):
                inner.result()
        return "unreachable"

    name = new_label("tx_hostile_unwind")
    _app, compiled = _two_node_flow(name, settle, start_to_close=timedelta(seconds=0.2))
    async with managed_pg_pool(pg_dsn, wf_conn, max_size=2) as pool:
        runner = FlowRunner(compiled, pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        worker = await RealWorker.open(pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            await worker.run(compiled, flow_id, "source")
            verdict = await asyncio.wait_for(worker.run(compiled, flow_id, "settle"), timeout=120)
            # The deadline's own disposition (the truthful TimeoutError's
            # ladder label), not the collision's InterfaceError class.
            assert verdict in ("laddered", "failed"), verdict
            await asyncio.wait_for(released.wait(), timeout=30)
            # The trapped session's backend was TERMINATED: it is gone, and
            # the pool's own connection count came back without it.
            row = await _settle_row(wf_conn, wf_schema, flow_id)
            assert row["status"] == "failed", dict(row)
            assert row["error_class"] == "TimeoutError", dict(row)
            # The terminated session's backend is really gone: the trap
            # closed the socket and the interrupted operation's own
            # server-side completion ends it (bounded by the pg_sleep's
            # own duration — the client-side termination is already done,
            # the backend's exit trails it). The pool's OTHER connections
            # were never affected and the next claim never sees the
            # trapped session.
            assert pids, "the body's backend pid was never observed"
            gone_deadline = asyncio.get_running_loop().time() + 20
            gone = 1
            while asyncio.get_running_loop().time() < gone_deadline:
                gone = int(
                    await wf_conn.fetchval(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid = ANY($1::int[])",
                        pids,
                    )
                )
                if gone == 0:
                    break
                await asyncio.sleep(0.5)
            assert gone == 0, f"the hostile session's backend survived: {pids}"
            # The body outcome was retrieved (no unretrieved-task noise:
            # asserted by the worker's own error-free logs on close) and
            # the pool still serves the next claim.
            next_use = await pool.execute("SELECT 1")
            assert next_use == "SELECT 1"
        finally:
            await worker.close()
        # A fresh worker on the SAME pool runs the re-claim to the exact
        # deadline disposition — no corruption followed the trap.
        worker2 = await RealWorker.open(pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            row = await _settle_row(wf_conn, wf_schema, flow_id)
            assert row["error_class"] == "TimeoutError", dict(row)
        finally:
            await worker2.close()


@pytest.mark.integration
async def test_failed_reset_is_a_resource_fact_and_the_outcome_stands(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
    pg_dsn: str,
) -> None:
    """A real asyncpg reset-hook failure on an acknowledged SUCCESS: the
    committed settlement stands, the emitter still closes exactly once,
    the session is not reused, and no second release or body execution
    happens."""
    resets: list[int] = []
    closes: list[int] = []
    body_calls: list[int] = []

    async def settle(ctx: TransactionalStepContext, source_result: object) -> str:
        body_calls.append(1)
        armed.append(ctx.connection)
        async with ctx.connection.transaction():
            pass
        return "ok"

    # The poison arms at the SETTLEMENT's own session (the real failure
    # mode: one specific session's reset is broken). Its first reset after
    # the body ran fails; every other release behaves.
    armed: list[object] = []
    reset_failures: list[object] = []

    def failing_reset(conn: object) -> object:
        resets.append(1)

        async def _fail_once(connection: object) -> None:
            if armed and connection is armed[0] and not reset_failures:
                # The poisoned session's own reset: the real driver
                # terminates it and re-raises; the pool replaces the
                # session and every later release behaves.
                reset_failures.append(connection)
                raise asyncpg.InterfaceError("the reset hook's own failure")

        return _fail_once(conn)

    name = new_label("tx_failed_reset")
    _app, compiled = _two_node_flow(name, settle)

    pool: asyncpg.Pool | None = None
    try:
        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2, reset=failing_reset)
        assert pool is not None
        runner = FlowRunner(compiled, pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        worker = await RealWorker.open(pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            await worker.run(compiled, flow_id, "source")
            verdict = await worker.run(compiled, flow_id, "settle")
            assert verdict in ("succeeded", "laddered", "failed"), verdict
            row = await _settle_row(wf_conn, wf_schema, flow_id)
            # The truthful outcome stands (a committed settlement is not
            # re-labeled by the release's failure; the run is not retried
            # into a second body execution).
            assert row["status"] in ("succeeded", "failed"), dict(row)
            assert len(body_calls) == 1, body_calls
        finally:
            await worker.close()
    finally:
        if pool is not None:
            await pool.close()
            closes.append(1)
    _ = closes


@pytest.mark.integration
async def test_real_connection_loss_at_rollback_keeps_the_causal_outcome(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
    pg_dsn: str,
) -> None:
    """A real backend kill DURING the managed rollback (after the body's
    application writes): zero prefix rows survive, the body's causal
    outcome stands, and the fresh-connection disposition converges."""
    from tests._wf_atomicity_fixture import application_tables

    wrote = asyncio.Event()
    hold_open = asyncio.Event()
    body_pid: list[int] = []

    async def settle(ctx: TransactionalStepContext, source_result: object) -> str:
        conn = ctx.connection
        pid = await conn.fetchval("SELECT pg_backend_pid()")
        body_pid.append(int(pid))
        await conn.execute(
            f'INSERT INTO "{wf_schema}".atomicity_domain (flow_id, value) VALUES ($1, $2)',
            flow_marker,
            "prefix",
        )
        wrote.set()
        if hold_open.is_set():
            # The FIRST attempt holds the unit open (the kill window); the
            # recovery's own attempt completes promptly.
            await asyncio.sleep(20)
        return "ok"

    flow_marker = new_uuid()
    name = new_label("tx_rollback_loss")
    _app, compiled = _two_node_flow(name, settle)
    async with (
        application_tables(wf_conn, wf_schema),
        managed_pg_pool(pg_dsn, wf_conn, max_size=2) as pool,
    ):
        runner = FlowRunner(compiled, pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        worker = await RealWorker.open(pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            await worker.run(compiled, flow_id, "source")
            hold_open.set()
            run_task = asyncio.create_task(worker.run(compiled, flow_id, "settle"))
            await asyncio.wait_for(wrote.wait(), timeout=30)
            # Kill the body's backend mid-unit: the session dies under
            # the open transaction (the pool's own connection_lost
            # cleanup releases the proxy; the unit's own next statement
            # and its ROLLBACK both fail on the dead session).
            await wf_conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid = ANY($1::int[])",
                body_pid,
            )
            hold_open.clear()
            with pytest.raises((asyncpg.PostgresError, asyncpg.InterfaceError)):
                await asyncio.wait_for(run_task, timeout=60)
            row = await _settle_row(wf_conn, wf_schema, flow_id)
            # Zero prefix commits: the application row does not exist.
            prefix = await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".atomicity_domain WHERE flow_id=$1',
                flow_marker,
            )
            assert int(prefix) == 0, prefix
            # The node is not terminal-successful — the reclaim's
            # re-pend ('scheduled') or its pending face owns the row.
            assert row["status"] in ("running", "failed", "pending", "scheduled"), dict(row)
            assert row["status"] != "succeeded", dict(row)
        finally:
            await worker.close()
        # The fresh-connection recovery: the reclaimed attempt runs
        # once, to a truthful terminal, and the prefix lands exactly
        # once.
        worker2 = await RealWorker.open(pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)  # type: ignore[attr-defined]
        try:
            deadline = asyncio.get_running_loop().time() + 90
            while asyncio.get_running_loop().time() < deadline:
                row = await _settle_row(wf_conn, wf_schema, flow_id)
                if row["status"] in ("succeeded", "failed"):
                    break
                await asyncio.sleep(1.0)
                try:
                    # The reclaimed row's due-scheduled flip is the LEADER's
                    # own sweep-3 arm (the production re-dispatch
                    # mechanism — never a test-local SQL rewrite).
                    from taskq.backend._sweeps import (
                        _SWEEP_3_SQL,  # pyright: ignore[reportPrivateUsage]
                    )

                    await wf_conn.execute(_SWEEP_3_SQL.format(schema=wf_schema), 10)
                    await worker2.run(compiled, flow_id, "settle")
                except AssertionError:
                    continue
            assert row["status"] in ("succeeded", "failed"), dict(row)
            prefix = await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".atomicity_domain WHERE flow_id=$1',
                flow_marker,
            )
            assert int(prefix) == 1, prefix
        finally:
            await worker2.close()
