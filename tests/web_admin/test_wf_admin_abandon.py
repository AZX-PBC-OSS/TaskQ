# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""The admin abandon door's pins (the operator abandon's HTTP face —
``POST /api/runs/{run_id}/abandon``):

- the delivered arm: a held hold's abandon → the ``DeliveryResult`` dict
  shape the resolve route returns, the node re-pends, and the audit row
  rides the CLIENT's own tx under the route's principal (the route never
  double-audits);
- the no-op arm: the second abandon reads the DEFINED no-op;
- the 404 arm: a hold that is not on the run is the honest 404;
- the timer refusal: a hold under the reserved ``__timer__`` prefix is
  refused (the clock is its only resolver) and the hold SURVIVES;
- the CSRF guard: a token-less POST never reaches the door.

Deliberately its own pack (the parallel builders own the other
web_admin files); every scenario drives the REAL pool/schema/client —
no stubbed seam.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import types
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration

MODULE_NAME = "p4_admin_abandon_flows"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


async def _wait(ctx: Any, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


@pytest.fixture
def abandon_app_module() -> Iterator[types.ModuleType]:
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("abandon_hold_flow")
    def hold_flow() -> Promise[object]:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    module.app = app_obj  # type: ignore[attr-defined]  # Why: the module:attr contract's dynamic half.
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _seed_held_run(pool: asyncpg.Pool, schema: str) -> str:
    compiled = sys.modules[MODULE_NAME].app.get("abandon_hold_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return str(flow_id)


def _csrf_of(client: Any) -> str:
    token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the GET must set the CSRF cookie before any POST"
    return token


async def test_route_delivers_the_operator_door_and_the_audit_rides_the_client_tx(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    abandon_app_module: Any,
) -> None:
    """The delivered arm: the route answers the resolve route's dict
    shape, the node re-pends, and the audit row is the CLIENT's own
    same-tx row (one row, the route's principal — never a double)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set
        first = await http.post(
            f"/api/runs/{run_id}/abandon",
            data={
                "hold_id": hold.hold_id,
                "reason": "the operator's stop",
                "csrf_token": _csrf_of(http),
            },
        )
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["status"] == "delivered", body
    assert body["reason"] is None, body
    # the hold: abandoned + the operator door's marker.
    row = await clean_pg_conn.fetchrow(
        f"SELECT status, payload->>'abandoned_by' AS door FROM \"{schema}\".wf_signals "
        "WHERE id = $1",
        uuid.UUID(hold.hold_id),
    )
    assert row is not None and row["status"] == "abandoned" and row["door"] == "operator", row
    # the audit: exactly ONE row, action hitl.abandon (the client's own tx).
    audits = await clean_pg_conn.fetch(
        f'SELECT action, target_id, reason FROM "{schema}".admin_audit '
        "WHERE action = 'hitl.abandon' AND target_id = $1 ORDER BY id",
        hold.hold_id,
    )
    assert len(audits) == 1, f"the abandon's audit is not exactly-once on the client's tx: {audits}"
    assert audits[0]["reason"] == "the operator's stop"


async def test_route_second_abandon_is_the_defined_no_op(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    abandon_app_module: Any,
) -> None:
    """The no-op arm: the second abandon reads the DEFINED no-op — the
    route answers it 200 with the client's verdict (never a 409 wall)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        token = _csrf_of(http)
        first = await http.post(
            f"/api/runs/{run_id}/abandon", data={"hold_id": hold.hold_id, "csrf_token": token}
        )
        assert first.status_code == 200 and first.json()["status"] == "delivered", first.text
        second = await http.post(
            f"/api/runs/{run_id}/abandon", data={"hold_id": hold.hold_id, "csrf_token": token}
        )
    assert second.status_code == 200, second.text
    assert second.json()["status"] == "no-op", second.json()


async def test_route_404s_a_hold_that_is_not_on_the_run(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    abandon_app_module: Any,
) -> None:
    """The 404 arm: a hold id that is not on THIS run (another run's, or
    one that names nothing) is the honest 404 — never a cross-run
    mutation."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    other_run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (other_hold,) = await client.list(other_run_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        token = _csrf_of(http)
        foreign = await http.post(
            f"/api/runs/{run_id}/abandon",
            data={"hold_id": other_hold.hold_id, "csrf_token": token},
        )
        assert foreign.status_code == 404, foreign.text
        unknown = await http.post(
            f"/api/runs/{run_id}/abandon",
            data={"hold_id": str(new_uuid()), "csrf_token": token},
        )
        assert unknown.status_code == 404, unknown.text
    # the foreign hold was untouched.
    still = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE id = $1',
        uuid.UUID(other_hold.hold_id),
    )
    assert still == "held", still


async def test_route_refuses_a_timer_hold_and_the_hold_survives(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    abandon_app_module: Any,
) -> None:
    """The timer refusal at the route: a hold under the reserved
    ``__timer__`` prefix is refused by the CLIENT's own door (the clock
    is its only resolver) — the answer is the refused dict shape, and
    the hold SURVIVES."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows.api._ctx_timer import TimerWake
    from taskq.workflows.api._hitl import TIMER_SIGNAL_PREFIX, register_timer_hold

    flow_id = uuid.UUID(run_id)
    node_id = await module_pg_pool.fetchval(
        f"SELECT id FROM \"{schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    async with module_pg_pool.acquire() as conn:
        timer_hold_id = await register_timer_hold(
            conn,
            schema=schema,
            workflow_id=JobId(flow_id),
            node_id=JobId(node_id),
            node_key="review",
            signal_name=f"{TIMER_SIGNAL_PREFIX}wake",
            hold_epoch=2,
            absolute=datetime.now(UTC) + timedelta(hours=1),
            relative=None,
            payload_schema={"TimerWake": TimerWake.model_json_schema()},
        )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        refused = await http.post(
            f"/api/runs/{run_id}/abandon",
            data={"hold_id": str(timer_hold_id), "csrf_token": _csrf_of(http)},
        )
    assert refused.status_code == 200, refused.text
    body = refused.json()
    assert body["status"] == "refused", body
    assert "timer" in body["reason"], body
    after = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE id = $1', timer_hold_id
    )
    assert after == "held", f"the refused abandon moved the timer hold: {after!r}"


async def test_route_refuses_a_post_without_the_csrf_token(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    abandon_app_module: Any,
) -> None:
    """The CSRF guard: a token-less POST never reaches the door (the
    same guarded-POST discipline the resolve route keeps)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        bare = await http.post(f"/api/runs/{run_id}/abandon", data={"hold_id": hold.hold_id})
    assert bare.status_code == 403, bare.text
    # the hold survived the refused POST.
    still = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".wf_signals WHERE id = $1',
        uuid.UUID(hold.hold_id),
    )
    assert still == "held", still


async def _node_id_of(pool: asyncpg.Pool, schema: str, run_id: str) -> uuid.UUID:
    node_id = await pool.fetchval(
        f"SELECT id FROM \"{schema}\".jobs WHERE step_key = 'review' AND "
        "(metadata->>'flow_id')::uuid = $1",
        uuid.UUID(run_id),
    )
    assert node_id is not None
    return uuid.UUID(str(node_id))


async def _wait_for_lock_wait(park: asyncpg.Connection, *, count: int) -> None:
    """The bounded condition wait: the route's writer is parked on the
    park's lock (a pg_stat_activity lock wait from another pid). Never a
    fixed sleep."""
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        waits = await park.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid()"
        )
        if waits and int(waits) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"the route's writer never queued on the park's lock ({count} wanted)")


async def test_route_answers_the_refused_verdict_when_the_writers_race(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    abandon_app_module: Any,
) -> None:
    """THE RACE'S HONEST LOSER AT THE ROUTE: a ``DeadlockDetectedError``
    out of the client's own transaction is the REFUSED verdict (the CLI
    verbs' named refusal is the wording model) — never a 500. The shape:
    a second connection holds the hold's wf_signals row FOR UPDATE; the
    route's abandon takes the node's job row (its pre-lock) and blocks on
    the signal row; the second connection then asks for that job row —
    the cycle; PG kills the route's writer; the route answers the refused
    dict."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    node_id = await _node_id_of(module_pg_pool, schema, run_id)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema)
    park = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as http:
            await http.get("/queues")
            token = _csrf_of(http)
            async with park.transaction():
                # THE PARK: the hold's signal row — the route's abandon
                # CAS queues behind it (its job-row pre-lock already
                # taken — the order both doors keep).
                await park.execute(
                    f'SELECT id FROM "{schema}".wf_signals WHERE id = $1 FOR UPDATE',
                    uuid.UUID(hold.hold_id),
                )
                post_task = asyncio.create_task(
                    http.post(
                        f"/api/runs/{run_id}/abandon",
                        data={
                            "hold_id": hold.hold_id,
                            "reason": "the raced stop",
                            "csrf_token": token,
                        },
                    )
                )
                await _wait_for_lock_wait(park, count=1)
                # THE CYCLE: the park now asks for the job row the
                # route's writer holds. PG kills the route's writer (its
                # wait began first — its detector fires first).
                await park.execute(
                    f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', node_id
                )
            response = await post_task
    finally:
        await park.close()
    assert response.status_code == 200, (
        f"the raced writer 500'd: {response.status_code} {response.text}"
    )
    body = response.json()
    assert body["status"] == "refused", body
    assert "writers raced" in body["reason"], body


async def test_resolve_route_answers_the_refused_verdict_when_the_writers_race(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    abandon_app_module: Any,
) -> None:
    """The resolve route's same arm (the pre-existing gap, the same
    cure): the resolve's writer killed by the deadlock answers the
    REFUSED verdict, never a 500."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    node_id = await _node_id_of(module_pg_pool, schema, run_id)
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    # the resolve route is the TYPED door: the definitions must be
    # mounted (the abandon route needs none — a stop-wait, no verdict).
    wf_app = abandon_app_module.app
    bundle = create_router(module_pg_pool, schema=schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    park = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as http:
            await http.get("/queues")
            token = _csrf_of(http)
            async with park.transaction():
                await park.execute(
                    f'SELECT id FROM "{schema}".wf_signals WHERE id = $1 FOR UPDATE',
                    uuid.UUID(hold.hold_id),
                )
                post_task = asyncio.create_task(
                    http.post(
                        f"/api/runs/{run_id}/resolve",
                        data={
                            "hold_id": hold.hold_id,
                            "decision": '{"verdict": "approve"}',
                            "csrf_token": token,
                        },
                    )
                )
                await _wait_for_lock_wait(park, count=1)
                await park.execute(
                    f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', node_id
                )
            response = await post_task
    finally:
        await park.close()
    assert response.status_code == 200, (
        f"the raced writer 500'd: {response.status_code} {response.text}"
    )
    body = response.json()
    assert body["status"] == "refused", body
    assert "writers raced" in body["reason"], body


async def test_route_malformed_id_audit_names_the_id_gate(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    abandon_app_module: Any,
) -> None:
    """The abandon route's malformed-id audit names the gate that refused
    (the id gate) — the abandon has NO payload gate, so the old
    ``admin-payload-gate`` label lied about the surface."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        refused = await http.post(
            f"/api/runs/{run_id}/abandon",
            data={"hold_id": "not-a-uuid", "csrf_token": _csrf_of(http)},
        )
    assert refused.status_code == 400, refused.text
    row = await clean_pg_conn.fetchval(
        f'SELECT detail FROM "{schema}".admin_audit '
        "WHERE action = 'hitl.abandon' ORDER BY id DESC LIMIT 1"
    )
    detail = json.loads(row) if isinstance(row, str) else row
    assert detail is not None and detail.get("stage") == "admin-hold-id-gate", detail
