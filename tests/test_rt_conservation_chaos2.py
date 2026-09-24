"""Conservation chaos, round 2: the NEW seams of the combined main composed.

The first sweep (tests/test_rt_conservation_chaos.py) composed the
individually-pinned failure families two at a time.  This round composes
the seams main gained since it, each pin two NEW seams deep, against real
Postgres (and Dragonfly where the broker rides), every pin carrying the
same CONSERVATION counter: jobs-in == terminal-out + in-flight-under-a-
live-lease + awaiting-reclaim, and the attempt ledger neither loses nor
fabricates a claim.

The compositions:

1. the cancel ladder's unheld-row walk x the claim-loss reconcile: a row
   whose outcome write was cancel-fenced (mark_retry's header: a
   phase-carrying row matches no arm) no-ops, the consumer's unconditional
   finally deregisters, and the row is running, locked, flagged, unheld.
   The ladder's unheld walk owes its abandon (whose fused INSERT writes
   the executed attempt's ledger row) while the reconcile reads the same
   row as "a claim that never reached an actor" and refunds the attempt
   whose body already ran.  ONE row, TWO owners: the pin holds only if
   the reconcile defers to the poll's own ownership contract.
2. the sighting map's drops x a claim->register handoff completing
   mid-drain: a row the poll stops returning drops its stamp (a
   reappearance re-observes with fresh graces, never a stale-elapsed
   escalation), and the unheld abandon's drain delivery resolves the
   REGISTRY's current entry, so a handoff completing between the
   queueing tick and the drain still takes its cancellation - identity-
   scoped, first delivery only.
3. the shield-retrieved abandon close x a worker SIGKILL mid-shield: the
   drain's abandon write is detached under the shield; the process dies
   inside the window and the write's fate (committed server-side or
   rolled back with the connection) must land the row on a conserved
   outcome either way - the next leader's reclaim owns the survivor.
4. the identity-fenced progress buffers x the cancel path's seq override
   under a re-claim mid-cancel: connection-kill chaos drives isolate,
   re-pend, and same-worker re-claim cycles while operator cancels force
   the stale attempts through the CancelledError handler's terminal seq
   override; the durable progress_seq must never regress and the
   body-run ledger must reconcile.

(The rolling-deploy generation-overlap shape x the new ladder lives in
the system tier: tests/system_e2e/test_ladder_rolling_deploy.py.)
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._protocol import JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client import JobsClient
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import CANCEL_ORIGIN_ABANDONED
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import (
    ModulePgSchema,
    _open_pg_backend_on_schema,  # pyright: ignore[reportPrivateUsage]  # Why: the driver's own backend on the module schema, the surface the first chaos round reaches through.
    redis_url_for,
)
from taskq.testing.health import unique_health_sock_path
from taskq.testing.pg import create_running_job
from taskq.worker.cancel import (
    _CancelController,  # pyright: ignore[reportPrivateUsage]  # Why: the pins drive the concrete controller tick by tick and read its sighting map.
)
from taskq.worker.run import _main
from tests.system_e2e._invariants import (
    conservation_violations,
    effect_ledger_violations,
)

pytestmark = [pytest.mark.integration]

_QUEUE = "consv2_q"
_TAG = "consv2"

#: The body-run ledger actors connect to PG with this DSN (set before a
#: worker starts; an actor body's surface is (payload, ctx) by contract,
#: so the DSN rides module state).
_RUN_STATE: dict[str, str] = {}

_EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".sys_effects (
    job_id  UUID NOT NULL,
    attempt INT NOT NULL,
    actor   TEXT NOT NULL,
    kind    TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""


class Consv2Payload(BaseModel):
    body_secs: float = 1.0
    calls: int = 4


async def _record_body_run(ctx: JobContext[Consv2Payload]) -> None:
    """Write the body-run effects row: one per (job_id, attempt) run."""
    run_dsn = _RUN_STATE.get("dsn")
    run_schema = _RUN_STATE.get("schema")
    assert run_dsn is not None and run_schema is not None
    conn = await asyncpg.connect(run_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{run_schema}".sys_effects (job_id, attempt, actor, kind) '
            "VALUES ($1, $2, $3, 'run')",
            ctx.job_id,
            ctx.attempt,
            ctx.actor,
        )
    finally:
        await conn.close()


@actor(name="consv2_fenced", queue=_QUEUE)
async def consv2_fenced(payload: Consv2Payload, ctx: JobContext[Consv2Payload]) -> None:
    """Runs, records, then raises: the retry write a cancel flag fences out."""
    await _record_body_run(ctx)
    await asyncio.sleep(payload.body_secs)
    raise RuntimeError("fenced-body-done")


@actor(name="consv2_progress", queue=_QUEUE)
async def consv2_progress(payload: Consv2Payload, ctx: JobContext[Consv2Payload]) -> None:
    # Progress FIRST (the seq surface is the seam under test while the
    # body runs), then the long tail; the effect lands only when the body
    # reaches its end (the completion-recording doctrine: an interrupted
    # attempt - the isolate's release, which writes no attempt row by
    # design - records nothing, and the re-run's row is the exactly-once
    # evidence).
    for step in range(payload.calls):
        await ctx.progress(step=step)
        await asyncio.sleep(0.15)
    await asyncio.sleep(payload.body_secs)
    await _record_body_run(ctx)


_CONSV2_REGISTRY: dict[str, object] = {
    "consv2_fenced": consv2_fenced,
    "consv2_progress": consv2_progress,
}


def _scoped_dsn(pg_dsn: str, schema: str) -> str:
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(pg_dsn)
    query = (
        f"application_name={schema}"
        if not parsed.query
        else f"{parsed.query}&application_name={schema}"
    )
    return urlunparse(parsed._replace(query=query))


def _worker_settings(pg_dsn: str, schema: str, **extra: object) -> WorkerSettings:
    base: dict[str, object] = {
        "pg_dsn": pg_dsn,
        "schema_name": schema,
        "heartbeat_interval": "0.5",
        "lock_lease": "8",
        "sweep_interval": "1",
        "poll_interval": "0.05",
        "cancellation_grace_period": "1",
        "cleanup_grace_period": "1",
        "heartbeat_command_timeout": "0.1",
        "watchdog_loop_lag_budget": "4.0",
        "watchdog_loop_lag_warn_budget": "0.5",
        "max_concurrency": "2",
        "queues": [_QUEUE],
        "health_socket_path": unique_health_sock_path("consv2"),
        "progress_coalesce_interval": "0.1",
    }
    base.update(extra)
    return WorkerSettings.load_from_dict(base)


async def _ensure_effects_table(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(_EFFECTS_DDL.format(schema=schema))


async def _drop_effects_table(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP TABLE IF EXISTS "{schema}".sys_effects')


async def _wait_running(
    conn: asyncpg.Connection, schema: str, tag: str, want: int, cap_secs: float
) -> list[UUID]:
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        rows = await conn.fetch(
            f'SELECT id FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
            tag,
        )
        if len(rows) >= want:
            return [r["id"] for r in rows]
        await asyncio.sleep(0.05)
    raise AssertionError(f"fewer than {want} jobs claimed within {cap_secs}s")


async def _settle_terminal(
    conn: asyncpg.Connection, schema: str, tag: str, cap_secs: float
) -> dict[str, int]:
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            f'SELECT count(*)::int AS n FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status NOT IN "
            "('succeeded', 'failed', 'crashed', 'cancelled', 'abandoned')",
            tag,
        )
        assert row is not None
        if row["n"] == 0:
            counts = await conn.fetch(
                f"SELECT status::text AS status, count(*)::int AS n "
                f'FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] GROUP BY status',
                tag,
            )
            return {r["status"]: r["n"] for r in counts}
        await asyncio.sleep(0.25)
    raise AssertionError(f"NOT SETTLED within {cap_secs}s: a dropped or livelocked job")


async def _assert_balanced_ledgers(conn: asyncpg.Connection, schema: str, tag: str) -> None:
    """The round's counter: conservation, the effects ledger, and the
    body-run doubles, over the tagged population, both tables."""
    violations = await conservation_violations(conn, schema, tag)
    assert not violations, "conservation violated:\n" + "\n".join(violations)
    effect = await effect_ledger_violations(conn, schema, tag)
    assert not effect, "the effects ledger does not reconcile:\n" + "\n".join(effect)


# ── Composition 1: the unheld-row walk x the claim-loss reconcile ───────


async def _run_unheld_walk_composition(
    pg_dsn: str,
    schema: str,
    tag: str,
    *,
    lock_lease: str,
    workers: int,
    jobs: int,
    **extra: object,
) -> dict[str, int]:
    """The shared body of composition 1.

    The flag arms while each body runs, the body's retry write matches no
    arm, the consumer deregisters, and the row is unheld, running,
    flagged.  From there TWO owners act on the ONE row: the ladder's
    unheld walk (graces, then the abandon whose fused INSERT writes the
    executed attempt's ledger row) and the reconcile (started_at older
    than the lease, nothing holds the row: refund the attempt, un-stamp
    started_at).  If the reconcile wins, the body run loses its claim
    row: the effects ledger's orphan class.
    """
    conn = await asyncpg.connect(pg_dsn)
    await _ensure_effects_table(conn, schema)
    dsn = _scoped_dsn(pg_dsn, schema)
    _RUN_STATE["dsn"] = pg_dsn
    _RUN_STATE["schema"] = schema
    settings = _worker_settings(dsn, schema, lock_lease=lock_lease, **extra)

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_CONSV2_REGISTRY)
        return 0

    worker_tasks = [asyncio.create_task(_runner(), name=f"consv2-c1-{i}") for i in range(workers)]
    try:
        await asyncio.sleep(2.0)  # bootstrap

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            for _ in range(jobs):
                await client.enqueue(consv2_fenced, Consv2Payload(body_secs=1.0), tags=[tag])
                await asyncio.sleep(0.1)

            job_ids = await _wait_running(conn, schema, tag, want=jobs, cap_secs=20.0)

            # The operator's request arms while the body runs (the body
            # sleeps a full second past this, so the retry write lands
            # flagged and the phase-2 arm never preempts it).
            for job_id in job_ids:
                await conn.execute(
                    f'UPDATE "{schema}".jobs SET cancel_phase = 1, '
                    "cancel_requested_at = clock_timestamp() WHERE id = $1",
                    job_id,
                )

            counts = await _settle_terminal(conn, schema, tag, cap_secs=90.0)
        finally:
            await stack.aclose()

        # The ladder owns the row the poll returns: the walk's abandon is
        # the terminal label (the reclaim sweep's cancel branch would say
        # 'cancelled', the refund path leaves the row to age into a
        # 'crashed' - neither is the walk's).
        assert counts.get("abandoned", 0) == jobs, (
            f"the unheld walk did not own every flagged row: {counts}"
        )

        # The executed attempt kept its ledger row: the abandon's fused
        # INSERT, not the reconcile's refund, decided the attempt.
        for job_id in job_ids:
            attempt = await conn.fetchval(
                f'SELECT attempt::int FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert attempt == 1, f"job {job_id}: attempt refunded to {attempt}"
            ledger = await conn.fetch(
                f'SELECT attempt, outcome FROM "{schema}".job_attempts '
                "WHERE job_id = $1 ORDER BY attempt",
                job_id,
            )
            assert [int(r["attempt"]) for r in ledger] == [1], (
                f"job {job_id}: the executed attempt lost its ledger row: "
                f"{[dict(r) for r in ledger]}"
            )
            assert ledger[0]["outcome"] == "cancelled", (
                f"job {job_id}: the abandon's fused attempt row mislabelled: "
                f"{ledger[0]['outcome']!r}"
            )
            error_class = await conn.fetchval(
                f'SELECT error_class FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert error_class == CANCEL_ORIGIN_ABANDONED, (
                f"job {job_id}: the abandon did not stamp its origin: {error_class!r}"
            )

        await _assert_balanced_ledgers(conn, schema, tag)

        # The composition fired: every body ran (the fenced class is only
        # reachable through a body that exited mid-flag).
        runs = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects '
            'WHERE job_id IN (SELECT id FROM "' + schema + '".jobs '
            "WHERE tags @> ARRAY[$1::text])",
            tag,
        )
        assert runs == jobs, f"expected {jobs} body runs, saw {runs}: composition did not fire"
        return counts
    finally:
        for task in worker_tasks:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, BaseException):
                    await asyncio.wait_for(task, timeout=60.0)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await _drop_effects_table(conn, schema)
        await conn.close()


@pytest.mark.parametrize("trial", range(3))
async def test_unheld_walk_x_claim_loss_reconcile_one_row_two_owners(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """Fleet-default lease: the walk's abandon lands well inside the
    reconcile's started_at age test; the pin holds and the executed
    attempt keeps its ledger row."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-c1-{trial}"
    await _run_unheld_walk_composition(pg_dsn, schema, tag, lock_lease="8", workers=1, jobs=2)


@pytest.mark.parametrize("trial", range(2))
async def test_unheld_walk_x_claim_loss_reconcile_contended_tight_lease(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """Contended: a lease tight enough that the reconcile's started_at
    age test comes due BEFORE the walk's graces have run.  Two workers,
    four fenced rows: the reconcile must defer every one of them to the
    poll's ownership contract, or an executed attempt loses its ledger
    row to a refund."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-c1x-{trial}"
    await _run_unheld_walk_composition(
        pg_dsn,
        schema,
        tag,
        lock_lease="3",
        workers=2,
        jobs=4,
        # The watchdog's lag budget must stay inside the tight lease
        # (the settings gate: budget + interval < lease).
        watchdog_loop_lag_budget="1.5",
        watchdog_loop_lag_warn_budget="0.3",
    )


# ── Composition 2: the sighting map's drops x the mid-drain handoff ─────


class _ControllerRig:
    """The real controller on real deps, driven tick by tick.

    ``open_worker_deps`` supplies the real pools and the real registry;
    the test supplies the clock by sleeping between ticks.
    """

    def __init__(
        self,
        stack: contextlib.AsyncExitStack,
        deps: Any,
        backend: Any,
        worker_id: UUID,
        schema: str,
    ) -> None:
        self.stack = stack
        self.deps = deps
        self.backend = backend
        self.worker_id = worker_id
        self.schema = schema
        self.controller: _CancelController = _CancelController(deps, worker_id, backend)

    async def tick(self) -> None:
        conn = await self.deps.heartbeat_pool.acquire()
        try:
            async with conn.transaction():
                await self.controller.run_in_tx(conn)
        finally:
            await self.deps.heartbeat_pool.release(conn)
        await self.controller.run_post_tx()

    async def aclose(self) -> None:
        await self.stack.aclose()


async def _make_rig(pg_dsn: str, schema: str, **extra: object) -> _ControllerRig:
    from taskq.testing.pg import create_worker
    from taskq.worker.deps import open_worker_deps

    settings = _worker_settings(_scoped_dsn(pg_dsn, schema), schema, **extra)
    stack = contextlib.AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await create_worker(conn, schema, worker_id)
    finally:
        await conn.close()
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=settings.cancellation_grace_period),
        cleanup_grace_period=timedelta(seconds=settings.cleanup_grace_period),
    )
    return _ControllerRig(stack, deps, backend, worker_id, schema)


class _StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


def _make_ctx(job_id: JobId, worker_id: UUID) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        claim_epoch=0,
        worker_id=worker_id,
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=str(job_id),
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


def _sleeper_task() -> asyncio.Task[object]:
    return asyncio.get_running_loop().create_task(asyncio.sleep(3600))


@pytest.mark.parametrize("trial", range(3))
async def test_sighting_map_drop_then_reappear_reobserves_fresh_graces(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """A flagged unheld row the poll stops returning drops its sighting
    stamp; when it reappears (re-armed), the graces re-observe from the
    reappearance, never from the stale first sight - a stale elapsed
    would escalate and abandon the re-armed row before ITS graces ran."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    rig = await _make_rig(pg_dsn, schema)
    job_id: UUID | None = None
    try:
        job_id = await create_running_job(
            conn,
            schema,
            rig.worker_id,
            cancel_phase=1,
            cancel_requested_at=datetime.now(UTC),
        )
        # Tick 1: first sight, the stamp is set. Graces are 1s; nothing
        # is due yet.
        await rig.tick()
        observed_at = rig.controller._unheld_observed_at.get(job_id)  # pyright: ignore[reportPrivateUsage]  # Why: the sighting map is the seam under test.
        assert observed_at is not None, "the first tick never sighted the unheld row"
        assert not rig.controller._pending_abandons  # pyright: ignore[reportPrivateUsage]

        # The poll stops returning the row: terminalise it out from under
        # the sighting map. Tick 2: the stamp must DROP.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'cancelled', finished_at = clock_timestamp() "
            "WHERE id = $1",
            job_id,
        )
        await rig.tick()
        assert job_id not in rig.controller._unheld_observed_at  # pyright: ignore[reportPrivateUsage]

        # The row reappears (re-armed at phase 1, running again): the
        # re-observation must be FRESH - one second out, the stale stamp
        # would have the escalation already due.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'running', finished_at = NULL, "
            "cancel_phase = 1, cancel_requested_at = clock_timestamp() "
            "WHERE id = $1",
            job_id,
        )
        await rig.tick()
        observed = rig.controller._unheld_observed_at.get(job_id)  # pyright: ignore[reportPrivateUsage]
        assert observed is not None, "the reappeared row was never re-sighted"
        assert asyncio.get_running_loop().time() - observed < 0.5, (
            "the reappeared row inherited a stale sighting stamp: its graces "
            "would run from the FIRST sight"
        )
        phase = await conn.fetchval(
            f'SELECT cancel_phase::int FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert phase == 1, f"the reappeared row was escalated on a stale stamp: phase {phase}"

        # Past the fresh graces the walk owns it again: escalation, then
        # the unheld abandon, whose fused INSERT writes the attempt row.
        await asyncio.sleep(1.2)
        await rig.tick()
        phase = await conn.fetchval(
            f'SELECT cancel_phase::int FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert phase == 2, f"the fresh observation never escalated: phase {phase}"
        await asyncio.sleep(1.2)
        await rig.tick()
        status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert status == "abandoned", f"the reappeared row never reached the abandon: {status}"
        ledger = await conn.fetch(
            f'SELECT attempt, outcome FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert len(ledger) == 1 and ledger[0]["outcome"] == "cancelled", (
            f"the abandon's fused attempt row did not reconcile: {[dict(r) for r in ledger]}"
        )
    finally:
        if job_id is not None:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
        await conn.close()
        await rig.aclose()


@pytest.mark.parametrize("trial", range(3))
async def test_drain_delivers_to_the_current_entry_mid_drain_handoff(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The unheld abandon's drain delivery resolves the REGISTRY's current
    entry: a claim->register handoff completing between the queueing tick
    and the drain still takes its cancellation, and the deregister drops
    exactly the entry the registry holds."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    rig = await _make_rig(
        pg_dsn, schema, cancellation_grace_period="0.05", cleanup_grace_period="0.05"
    )
    job_id: UUID | None = None
    task: asyncio.Task[object] | None = None
    try:
        job_id = await create_running_job(
            conn,
            schema,
            rig.worker_id,
            cancel_phase=2,
            cancel_requested_at=datetime.now(UTC),
        )
        # First sight, then graces (0.05s + 0.05s) in a second tick: the
        # walk queues the unheld abandon, the drain applies it, the row
        # is 'abandoned' before any entry exists.
        await rig.tick()
        await asyncio.sleep(0.15)
        await rig.tick()
        status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert status == "abandoned", f"the unheld abandon never applied: {status}"

        # The handoff: a live attempt registers the SAME key (a re-claim
        # took the row and its registry entry). A LATER drain delivery
        # for that row resolves the CURRENT entry.
        live_started = asyncio.Event()
        live_cancelled = asyncio.Event()

        async def _live_body() -> None:
            live_started.set()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(3600)
            live_cancelled.set()

        task = asyncio.create_task(_live_body())
        await live_started.wait()
        await rig.deps.active_jobs.register(job_id, task, _make_ctx(job_id, rig.worker_id))

        # Queue a SECOND abandon for the same row through the walk (the
        # row is terminal now, so seed the queue directly: the drain's
        # delivery is the seam under test, the queueing arm is pinned by
        # the walk tests above). queued_entry is None: the unheld class.
        rig.controller._pending_abandons.append((job_id, None))  # pyright: ignore[reportPrivateUsage]
        await rig.controller.run_post_tx()
        # The delivery's task.cancel() is not awaited; yield so the live
        # task processes its cancellation before the assertions read it.
        await asyncio.sleep(0.05)

        assert live_cancelled.is_set(), (
            "the drain's delivery never cancelled the CURRENT entry: a "
            "mid-drain handoff would run on unaware"
        )
        assert rig.deps.active_jobs.get(job_id) is None, (
            "the delivery left the live entry registered: every held_ids reader misreads the map"
        )
    finally:
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await task
        if job_id is not None:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
        await conn.close()
        await rig.aclose()


@pytest.mark.parametrize("trial", range(2))
async def test_abandon_attempt_insert_survives_an_unstamped_row(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The abandon's fused attempt INSERT carries the per-row clock
    fallback for a NULL started_at (the reclaim sweep's INSERT contract):
    an un-stamped row must reach its abandon, never wedge the heartbeat
    loop on a NotNullViolation the drain re-queues forever."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    rig = await _make_rig(
        pg_dsn, schema, cancellation_grace_period="0.05", cleanup_grace_period="0.05"
    )
    job_id: UUID | None = None
    try:
        job_id = await create_running_job(
            conn,
            schema,
            rig.worker_id,
            cancel_phase=2,
            cancel_requested_at=datetime.now(UTC),
        )
        # The un-stamped shape: the claim's stamp named an execution that
        # never started (the reconcile's refund leaves exactly this row
        # behind), here seeded directly.
        await conn.execute(f'UPDATE "{schema}".jobs SET started_at = NULL WHERE id = $1', job_id)
        await rig.tick()
        await asyncio.sleep(0.15)
        await rig.tick()
        status = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert status == "abandoned", f"the abandon never applied to the un-stamped row: {status}"
        ledger = await conn.fetch(
            f'SELECT attempt, outcome, started_at FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert len(ledger) == 1 and ledger[0]["outcome"] == "cancelled", (
            f"the fused attempt row did not reconcile: {[dict(r) for r in ledger]}"
        )
        assert ledger[0]["started_at"] is not None, "the fallback never stamped the row"
    finally:
        if job_id is not None:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
        await conn.close()
        await rig.aclose()


@pytest.mark.parametrize("trial", range(2))
async def test_drain_delivery_contended_replaced_entry_never_takes_two_cancels(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """Contended: the key changes hands between two registrations before
    the drain. The delivery resolves the CURRENT entry only: the replaced
    (stale) task takes no second cancellation from an abandon it never
    queued, and the registry drops the live entry."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    rig = await _make_rig(
        pg_dsn, schema, cancellation_grace_period="0.05", cleanup_grace_period="0.05"
    )
    job_id: UUID | None = None
    stale_task: asyncio.Task[object] | None = None
    live_task: asyncio.Task[object] | None = None
    try:
        job_id = await create_running_job(
            conn,
            schema,
            rig.worker_id,
            cancel_phase=2,
            cancel_requested_at=datetime.now(UTC),
        )
        await rig.tick()
        await asyncio.sleep(0.15)
        await rig.tick()
        assert (
            await conn.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id)
            == "abandoned"
        )

        cancelled: list[asyncio.Task[object]] = []

        def _track_cancelled(task: asyncio.Task[object]) -> None:
            if task.cancelled():
                cancelled.append(task)

        stale_task = _sleeper_task()
        stale_task.add_done_callback(_track_cancelled)
        await rig.deps.active_jobs.register(job_id, stale_task, _make_ctx(job_id, rig.worker_id))

        # The re-claim replaces the key with the live attempt's entry
        # (register's key-scoped absorb). The stale entry is NOT the
        # registry's any more.
        live_task = _sleeper_task()
        live_task.add_done_callback(_track_cancelled)
        await rig.deps.active_jobs.register(job_id, live_task, _make_ctx(job_id, rig.worker_id))

        rig.controller._pending_abandons.append((job_id, None))  # pyright: ignore[reportPrivateUsage]
        await rig.controller.run_post_tx()
        await asyncio.sleep(0.05)

        assert live_task in cancelled, "the delivery missed the CURRENT entry"
        assert stale_task not in cancelled, (
            "the delivery cancelled the STALE entry too: a task that never "
            "queued this abandon took a second cancellation"
        )
        assert rig.deps.active_jobs.get(job_id) is None
    finally:
        for t in (stale_task, live_task):
            if t is not None:
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, BaseException):
                    await t
        if job_id is not None:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
        await conn.close()
        await rig.aclose()


# ── Composition 3: the shield-retrieved close x a SIGKILL mid-shield ────


def _spawn_consv2_worker(
    pg_dsn: str,
    schema: str,
    socket_path: str,
    *,
    kill_on_abandon: bool = False,
) -> subprocess.Popen[bytes]:
    env = {**os.environ}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_QUEUES": _QUEUE,
            "TASKQ_POLL_INTERVAL": "0.05",
            "TASKQ_SWEEP_INTERVAL": "1",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "3.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_HEALTH_SOCKET_PATH": socket_path,
            "TASKQ_WATCHDOG_ENABLED": "false",
            "TASKQ_MAX_CONCURRENCY": "2",
        }
    )
    if kill_on_abandon:
        env["TASKQ_CONSV2_KILL_ON_ABANDON"] = "1"
    run_dsn = _RUN_STATE.get("dsn")
    run_schema = _RUN_STATE.get("schema")
    if run_dsn is not None and run_schema is not None:
        env["TASKQ_CONSV2_RUN_DSN"] = run_dsn
        env["TASKQ_CONSV2_RUN_SCHEMA"] = run_schema
    return subprocess.Popen(  # Why: fixed argv, project-owned module.
        [sys.executable, "-m", "tests._worker_harness_consv2"],
        env=env,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )


def _wait_for_socket(socket_path: str, proc: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _stdout, stderr = proc.communicate(timeout=1)
            raise RuntimeError(f"worker exited rc={proc.returncode} stderr={stderr.decode()!r}")
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(0.1)
                sock.connect(socket_path)
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"socket {socket_path!r} did not appear within 15s")


@pytest.mark.timeout(180)
@pytest.mark.parametrize("trial", range(2))
async def test_sigkill_mid_shield_the_detached_abandon_conerves(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The drain's abandon write is detached under ``shield_with_retrieval``;
    the process dies INSIDE the shield window. The write either committed
    server-side (row 'abandoned', fused attempt row present) or rolled
    back with the dying connection (row still running, flagged, lease
    lapsing; the next leader's reclaim owns it). Both shapes are
    conserved; a third shape (a body run with no attempt row behind it, a
    running row nobody owns) is a drop."""
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-c3-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    await _ensure_effects_table(conn, schema)
    _RUN_STATE["dsn"] = pg_dsn
    _RUN_STATE["schema"] = schema
    dsn = _scoped_dsn(pg_dsn, schema)
    sock = unique_health_sock_path(f"consv2-c3-{trial}")
    proc = _spawn_consv2_worker(dsn, schema, sock, kill_on_abandon=True)
    job_id: UUID | None = None
    try:
        _wait_for_socket(sock, proc)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            handle = await client.enqueue(consv2_fenced, Consv2Payload(body_secs=0.5), tags=[tag])
            job_id = handle.job_id

            running = await _wait_running(conn, schema, tag, want=1, cap_secs=20.0)
            assert job_id in running
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 1, '
                "cancel_requested_at = clock_timestamp() WHERE id = $1",
                job_id,
            )

            # The walk escalates at +1s and queues the abandon at +2s;
            # the injection kills the process mid-shield.
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline and proc.poll() is None:  # noqa: ASYNC110  # Why: a subprocess exit is not an asyncio primitive; the poll must read proc.poll().
                await asyncio.sleep(0.2)
            proc.wait(timeout=10)
            assert proc.returncode == -9, (
                f"the harness exited {proc.returncode} instead of dying by SIGKILL "
                "inside the shield window: the injection never fired"
            )
        finally:
            await stack.aclose()

        # The survivor's owner: the write either landed or it did not.
        # Settle the row to terminal the way the fleet does: the lease
        # (3s) lapses and the reclaim sweep's cancel branch owns whatever
        # is still running.
        from taskq.backend._sweeps import sweep_expired_locks

        deadline = time.monotonic() + 30.0
        status: str | None = None
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            await sweep_expired_locks(
                conn,
                timedelta(seconds=1),
                timedelta(seconds=1),
                schema=schema,
            )
            status = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
            )
            if status in ("abandoned", "cancelled"):
                break
        assert status in ("abandoned", "cancelled"), (
            f"the mid-shield row never reached a conserved terminal: {status}"
        )

        # Either shape: the ledger reconciles and the body run has its
        # claim row behind it.
        await _assert_balanced_ledgers(conn, schema, tag)
        runs = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects WHERE job_id = $1',
            job_id,
        )
        assert runs == 1, f"expected exactly one body run, saw {runs}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock)
        if job_id is not None:
            await conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1', job_id)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await _drop_effects_table(conn, schema)
        await conn.close()


# ── Composition 4: the identity-fenced buffers x the cancel seq override ─


@pytest.mark.parametrize("trial", range(2))
async def test_cancel_seq_override_survives_a_same_worker_reclaim(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    killable_redis_container: object,
    trial: int,
) -> None:
    """A live row is re-pended out from under its consumer (the lease
    lapse the reclaim sweep writes, seeded the sanctioned way) and the
    SAME worker re-claims it: the new attempt installs its buffer under
    the same key, then the operator's cancel wave drives the LADDER's
    delivery - every unwind reading the buffer ITS OWN attempt installed.
    The seq surface must survive the composition: the durable
    progress_seq never regresses, it ADVANCES again after the re-claim
    (a stale exit that evicted the live buffer would stall the flush
    loop), and the ledgers reconcile through the re-claim."""
    redis_url = redis_url_for(killable_redis_container)
    schema = module_pg_schema.schema_name
    tag = f"{_TAG}-c4-{trial}"
    conn = await asyncpg.connect(pg_dsn)
    await _ensure_effects_table(conn, schema)
    dsn = _scoped_dsn(pg_dsn, schema)
    settings = _worker_settings(
        dsn,
        schema,
        max_concurrency="4",
        redis_url=redis_url,
        # The watchdog's lag budget must stay inside the tight lease
        # (the settings gate: budget + interval < lease).
        watchdog_loop_lag_budget="1.5",
        watchdog_loop_lag_warn_budget="0.3",
    )
    _RUN_STATE["dsn"] = pg_dsn
    _RUN_STATE["schema"] = schema

    seq_samples: dict[str, int] = {}

    async def _sample_seq() -> int:
        """The durable seq never regresses, sampled through the chaos."""
        rows = await conn.fetch(
            f'SELECT id::text AS id, progress_seq FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text]",
            tag,
        )
        high = 0
        for row in rows:
            jid = row["id"]
            seq = row["progress_seq"] or 0
            if seq < seq_samples.get(jid, 0):
                raise AssertionError(
                    f"job {jid}: the durable progress_seq regressed "
                    f"({seq_samples[jid]} -> {seq}): a stale attempt's write "
                    "evicted or overrode the live buffer"
                )
            seq_samples[jid] = max(seq_samples.get(jid, 0), seq)
            high = max(high, seq)
        return high

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_CONSV2_REGISTRY)
        return 0

    worker_task = asyncio.create_task(_runner(), name=f"consv2-c4-{trial}")
    job_ids: list[UUID] = []
    try:
        await asyncio.sleep(2.0)

        stack, _deps, backend = await _open_pg_backend_on_schema(pg_dsn, schema)
        try:
            client = JobsClient(backend)
            for _ in range(2):
                handle = await client.enqueue(
                    consv2_progress,
                    Consv2Payload(body_secs=8.0, calls=20),
                    tags=[tag],
                )
                job_ids.append(handle.job_id)
                await asyncio.sleep(0.2)

            job_ids = await _wait_running(conn, schema, tag, want=2, cap_secs=20.0)
            before_reclaim = await _sample_seq()
            assert before_reclaim >= 1, "no progress ever flushed before the re-claim"

            # The re-claim, seeded the sanctioned way: the exact row shape
            # the reclaim sweep's re-pend arm writes (pending, unlocked,
            # claimable now), applied while the consumers run. The
            # worker's dispatch re-claims within a poll interval and the
            # new attempts install THEIR buffers under the same keys.
            for job_id in job_ids:
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'pending', "
                    "locked_by_worker = NULL, lock_expires_at = NULL, "
                    "last_heartbeat_at = NULL, cancel_phase = 0, "
                    "cancel_requested_at = NULL, assignment_routed = true, "
                    "scheduled_at = clock_timestamp() WHERE id = $1",
                    job_id,
                )

            # The re-claim fired: the attempt counter moved, and the NEW
            # attempts' progress flushes advance the durable surface past
            # the stale attempts' tail (the fence's substance: a stale
            # exit that evicted the live buffer would stall it).
            attempts = 0
            reclaimed_by = time.monotonic() + 20.0
            while time.monotonic() < reclaimed_by:
                attempts = await conn.fetchval(
                    f'SELECT count(*)::int FROM "{schema}".jobs '
                    "WHERE tags @> ARRAY[$1::text] AND attempt >= 2",
                    tag,
                )
                if attempts == len(job_ids):
                    break
                await asyncio.sleep(0.1)
            assert attempts == len(job_ids), (
                f"the same worker never re-claimed the rows: {attempts} of "
                f"{len(job_ids)} moved to a second attempt"
            )

            advanced_by = time.monotonic() + 20.0
            advanced = False
            while time.monotonic() < advanced_by:
                after = await _sample_seq()
                if after > before_reclaim + 1:
                    advanced = True
                    break
                await asyncio.sleep(0.2)
            assert advanced, (
                f"the durable seq never advanced past the re-claim "
                f"({before_reclaim}): the stale exits evicted the live "
                "buffers and the flush loop stopped draining them"
            )

            # The cancel wave: phase 2 arms on the LIVE (re-claimed)
            # attempts; the ladder's delivery unwinds them through the
            # terminal seq override, each reading the buffer it installed.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 2, '
                "cancel_requested_at = clock_timestamp() "
                "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
                tag,
            )
            counts = await _settle_terminal(conn, schema, tag, cap_secs=120.0)
            await _sample_seq()
            _ = counts
        finally:
            await stack.aclose()

        # The ledgers. The re-claim was seeded (no lock_expired events of
        # its own); the exactly-once core is the doubles check, and every
        # recorded run must sit on an attempt the counter charged.
        violations = await conservation_violations(conn, schema, tag)
        assert not violations, "conservation violated:\n" + "\n".join(violations)
        doubles = await conn.fetchval(
            f"SELECT count(*)::int FROM (SELECT job_id, attempt FROM "
            f'"{schema}".sys_effects GROUP BY job_id, attempt '
            "HAVING count(*) > 1) d",
        )
        assert doubles == 0, f"{doubles} (job, attempt) pairs ran the body twice"
        uncharged = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".sys_effects e '
            'WHERE e.job_id IN (SELECT id from "'
            + schema
            + '".jobs WHERE tags @> ARRAY[$1::text]) AND e.attempt > '
            '(SELECT attempt FROM "' + schema + '".jobs j WHERE j.id = e.job_id)',
            tag,
        )
        assert uncharged == 0, (
            f"{uncharged} body runs sit above their job's attempt counter: a "
            "run the claim never charged"
        )
        # The composition fired in both directions: the re-claims ran the
        # bodies again (the attempt ledger carries the second attempts,
        # however they terminalised), and the re-runs' progress advanced
        # the durable surface (asserted above).
        reruns = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".job_attempts a '
            f'JOIN "{schema}".jobs j ON j.id = a.job_id '
            "WHERE j.tags @> ARRAY[$1::text] AND a.attempt >= 2",
            tag,
        )
        assert reruns >= 1, "no body ever re-ran under a second attempt"
    finally:
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]', tag)
        await _drop_effects_table(conn, schema)
        await conn.close()
