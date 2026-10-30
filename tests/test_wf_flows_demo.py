"""The flows demo's smoke test (ledger E1): the workflow feature's
end-to-end story, run IN-PROCESS through the dev door (``FlowRunner``/
``drive`` — the same driver the docs fence and ``examples.workflows``
use; the rows are the same rows a worker process would drive).

One run of ``examples/flows_demo/app.py``'s graph proves:

* the SOURCE step fans the map out and the DOOMED item fails while the
  flow COMPLETES — the failing item fans in as the typed
  ``FailureInfo`` envelope (``policy: "collect"``) on the map join
  row's ``metadata.failures`` (the failing-item collect);
* the HITL gate actually HOLDS (drive #1 answers ``held``), the typed
  resolve delivers (``HitlClient.resolve`` — the admin Resolve form's
  rows), and the review node resumes to terminal;
* the DURABLE TIMER fires from the sweep's timer arm
  (``fired_by=timer`` on the resumed body's record);
* the CAPPED stage's terms ride the handle's declaration
  (``@app.actor(max_concurrent=1, …)``) into the node decl and the
  ``actor_config`` row, and the not-thread-safe resource's overlap
  guard NEVER fires (the cap's tripwire stays silent for the whole
  run);
* the run leaves rows in every table the retention arms will sweep.

Marked ``examples`` (the example tests' own marker — see
``tests/test_examples_smoke.py``) + ``integration`` (testcontainers PG;
no Redis — the workflow holds ride pg_notify, the demo needs no
broker).
"""

from __future__ import annotations

import json
import os
from typing import Any, cast

import asyncpg
import pytest
from examples.flows_demo import run_demo as run_demo_module
from examples.flows_demo.app import (
    DOOMED_ITEM,
    PDF_RESERVATION_NAME,
    SERIAL_GUARD,
    configure,
    flows_app,
)

from taskq.ratelimit.reservation import ConcurrencyReservation

pytestmark = [pytest.mark.examples, pytest.mark.integration]

#: The demo's queue universe (the zero-warning compile's declaration).
_DEMO_QUEUES = "flows-src,flows-review,flows-timer,flows-pub"

_NodeRows = dict[tuple[str, int | None], dict[str, Any]]


@pytest.fixture
def demo_env(monkeypatch: pytest.MonkeyPatch, module_pg_schema: Any) -> None:
    """The demo's launch env: the queues declared, the retention arms
    armed (the demo's launch surface), the schema bound before anything
    compiles."""
    monkeypatch.setenv("TASKQ_QUEUES", _DEMO_QUEUES)
    for name, value in run_demo_module.RETENTION_ENV.items():
        monkeypatch.setenv(name, value)
    configure(module_pg_schema.schema_name)


def _decoded(meta: Any) -> dict[str, Any]:
    """The jsonb read's decode (asyncpg hands uncoded connections a str)."""
    return json.loads(meta) if isinstance(meta, str) else meta


async def _drive_one_run(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> tuple[Any, Any, dict[str, Any], _NodeRows]:
    """One full demo run through the dev door: create → the HOLD → the
    typed resolve → the timer → the capped publish → the terminal
    report. Returns (run_id, digest, the map join's metadata, the node
    rows keyed (step_key, map_index))."""
    from examples.flows_demo.app import Digest

    from taskq.workflows import FlowRunner, HitlClient

    runner = FlowRunner(flows_app.get("flows_demo"), wf_pool, wf_schema)
    claim = await runner.create_flow()
    run_id = claim.flow_id
    assert claim.kind == "created"

    # THE FIRST WALL: the drive stops at the review HOLD — the HITL
    # gate minted its typed hold (the hold row is the truth).
    state = await runner.drive(run_id, until="held", max_ticks=500)
    assert state == "held", f"the drive ended {state!r} before the review hold"

    client = HitlClient(wf_pool, wf_schema)
    holds = await client.list(run_id)
    assert len(holds) == 1, f"expected the review hold, got {holds}"
    assert holds[0].signal_name == "Approval", holds[0].signal_name

    # THE RESOLVE (the admin Resolve form's rows): the typed payload
    # delivers through the gate's declared model.
    verdict = await client.resolve(
        holds[0].hold_id,
        {"verdict": "approve", "note": "ship it"},
        principal="smoke:flows-demo",
    )
    assert verdict.status == "delivered", verdict

    # THE RESUME: the timer fires from the sweep's arm, the capped
    # publish lands, the run terminalizes.
    state = await runner.drive(run_id, until="terminal", max_ticks=3000)
    assert state == "terminal", f"the resume ended {state!r} — a bounded stop is a defect"

    # THE TERMINAL REPORT (the envelope is the truth).
    digest = Digest.model_validate(await runner.result(run_id))

    join_row = await wf_pool.fetchrow(
        f'SELECT status, metadata FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source.join'",
        run_id,
    )
    assert join_row is not None, "the map join row is missing"

    nodes: _NodeRows = {
        (row["step_key"], row["map_index"]): dict(row)
        for row in await wf_pool.fetch(
            f'SELECT step_key, status, attempt, map_index FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
            "WHERE (metadata->>'flow_id')::uuid = $1",
            run_id,
        )
    }
    return run_id, digest, _decoded(join_row["metadata"]), nodes


async def test_demo_flow_end_to_end(
    demo_env: None,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """ONE run: fan-out → the doomed item's collect → the HOLD → the
    typed resolve → the timer → the capped publish → the terminal
    report. The overlap guard never fires; the root COMPLETES."""
    run_id, digest, join_meta, nodes = await _drive_one_run(wf_pool, wf_schema)

    # THE COLLECT'S WHOLE POINT: a failing item does NOT fail the flow.
    assert DOOMED_ITEM not in digest.published
    assert digest.published == sorted(digest.published)
    assert "approve" in digest.review_note and "ship it" in digest.review_note
    assert "fired_by=timer" in digest.cooldown, digest.cooldown

    root = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
        run_id,
    )
    assert root == "succeeded", root

    # THE COLLECTED FAILURE: the map join row's metadata.failures names
    # the doomed item with the typed FailureInfo envelope — the policy
    # marker reads 'collect' (the envelope never lies about which
    # policy ran).
    assert join_meta.get("failures") is not None, join_meta
    failures = join_meta["failures"]
    assert len(failures) == 1, f"expected the doomed item's one collected failure, got {failures}"
    envelope = failures[0]
    assert envelope["node_key"] == "source.item"
    assert envelope["policy"] == "collect"
    assert envelope["error"]["error_class"] == "RuntimeError"
    assert DOOMED_ITEM in envelope["error"]["error_message"]

    # THE DOOMED ITEM'S OWN ROW: terminal-failed at its ladder bound
    # (the map's max_attempts=1 — one attempt, no retry storm); the
    # SURVIVORS all succeeded.
    doomed = nodes.get(("source.item", envelope["map_index"]))
    assert doomed is not None, sorted(nodes)
    assert doomed["status"] == "failed", doomed
    assert doomed["attempt"] == 1, doomed
    survivors = [
        row for (key, _idx), row in nodes.items() if key == "source.item" and row is not doomed
    ]
    assert len(survivors) == 4
    assert all(row["status"] == "succeeded" for row in survivors), survivors

    # THE CAP'S TRIPWIRE: the not-thread-safe resource was never
    # entered twice — the guard is silent for the whole run.
    assert SERIAL_GUARD.tripped == [], SERIAL_GUARD.tripped
    assert SERIAL_GUARD.entries >= 5  # four enrich items + the publish


async def test_capacity_terms_declared(demo_env: None) -> None:
    """The capacity surface's DECLARATION: the capped step's terms ride
    the wiring into the node decl (the same terms
    ``project_workflow_actor_configs`` projects into ``actor_config``).
    The reservation arrives as the declared ``ConcurrencyReservation``;
    the TTLs and bounds arrive as declared."""
    compiled = flows_app.get("flows_demo")
    node = compiled.nodes["report"]
    assert node.max_concurrent == 1
    assert node.result_ttl == 3600.0
    assert [type(r).__name__ for r in node.reservations] == ["ConcurrencyReservation"]
    reservation = cast("ConcurrencyReservation", node.reservations[0])
    assert reservation.name == PDF_RESERVATION_NAME
    assert reservation.slots == 1

    # The queue placement: the capped stage is its OWN cohort (the
    # one-queue-per-actor law's shape).
    assert (node.actor, node.queue) == ("flows-pub", "flows-pub")


async def test_capacity_terms_land_in_actor_config(
    demo_env: None,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The capacity terms' ACTOR_CONFIG face: the boot projection
    carries the capped cohort's terms into the row (the seed-only law —
    the declaration seeds, the operator owns thereafter). The SAME
    two-phase upsert the worker boot runs lands the rows; the projection
    reads the app through the registry the autouse isolation clears —
    the demo's app re-registered for THIS test's projection."""
    from taskq.worker.startup import sync_actor_config
    from taskq.workflows._worker_execution import (
        project_workflow_actor_configs,
        register_app,
    )

    register_app(flows_app)  # the autouse isolation cleared the registry at setup
    configs = list(project_workflow_actor_configs())
    by_actor = {config.actor: config for config in configs}

    pub = by_actor["flows-pub"]
    assert pub.queue == "flows-pub"
    assert pub.max_concurrent == 1
    assert (
        pub.max_pending is None
    )  # H-A6: max_pending is off the wf surface — the row seeds it UNSET
    assert pub.result_ttl == 3600.0
    assert pub.metadata.get("workflow_actor") is True, pub.metadata

    # The row lands through the worker boot's own upsert (a CONNECTION —
    # the sync runs its own transactions).
    async with wf_pool.acquire() as conn:
        await sync_actor_config(conn, configs, schema=wf_schema)

    row = await wf_pool.fetchrow(
        "SELECT queue, max_concurrent, max_pending, result_ttl, metadata "  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
        f'FROM "{wf_schema}".actor_config WHERE actor = $1',
        "flows-pub",
    )
    assert row is not None, "the capped cohort's actor_config row is missing"
    assert row["queue"] == "flows-pub"
    assert row["max_concurrent"] == 1
    assert (
        row["max_pending"] is None
    )  # H-A6: the dead knob is off the surface — the row seeds UNSET
    assert row["result_ttl"] == 3600.0
    meta = _decoded(row["metadata"])
    assert meta.get("workflow_actor") is True, meta


async def test_retention_arms_targets_exist(
    demo_env: None,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
) -> None:
    """The retention arms' TARGETS exist and the demo run fills them:
    the run leaves ``wf_edge`` / ``wf_step_ledger`` / ``wf_signals`` /
    ``wf_node_progress`` rows — the five bounded-batch sweeps' scan
    targets — and ONE pass of the arms at the demo's DECLARED periods
    (7d) deletes NOTHING (the rows are fresh; the arms' age clocks have
    not run past the launch surface's values)."""
    run_id, _digest, _join_meta, _nodes = await _drive_one_run(wf_pool, wf_schema)

    edge = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_edge WHERE flow_id = $1',  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
        run_id,
    )
    ledger = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',  # noqa: S608  # Why: as above — the bound parameter carries the run id.
        run_id,
    )
    signals = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals WHERE workflow_id = $1',  # noqa: S608  # Why: as above — the bound parameter carries the run id.
        run_id,
    )
    progress = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_node_progress p '  # noqa: S608  # Why: the schema is the fixture's settings-validated identifier; every value is a bound parameter.
        f'JOIN "{wf_schema}".jobs j ON p.node_id = j.id '
        "WHERE j.metadata->>'flow_id' = $1",  # the ->> read is text — bind the run id's spelling
        str(run_id),
    )
    assert edge >= 4, "the demo run left no edge-ledger rows — nothing for the wf_edge arm to sweep"
    assert ledger >= 4, (
        "the demo run left no step-ledger rows — nothing for the wf_step_ledger arm to sweep"
    )
    assert signals >= 2, (
        "the demo run left no signal rows (the Approval hold + the timer hold) — "
        "nothing for the wf_signals arm to sweep"
    )
    assert progress >= 1, (
        "the demo run left no node-progress rows — nothing for the wf_node_progress arm to sweep"
    )

    # ONE PASS OF THE ARMS at the demo's declared periods: the leader's
    # own sweep functions, the launch surface's values — a fresh run's
    # history is not aged out (the zero-means-off convention's positive
    # face: the arms run, nothing is old enough to die).
    from datetime import timedelta

    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._sweep import (
        prune_wf_edge,
        prune_wf_join_fire,
        prune_wf_node_progress,
        prune_wf_signals,
        prune_wf_step_ledger,
    )

    wsql = WorkflowSql.build(wf_schema)
    week = timedelta(days=7)
    assert await prune_wf_edge(wf_pool, wsql, retention=week) == 0
    assert await prune_wf_join_fire(wf_pool, wsql, retention=week) == 0
    assert await prune_wf_step_ledger(wf_pool, wsql, retention=week) == 0
    assert await prune_wf_signals(wf_pool, wsql, retention=week) == 0
    assert await prune_wf_node_progress(wf_pool, wsql, retention=week) == 0


def test_retention_env_is_the_demo_launch_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    """The demo's launch surface: the retention arms arm through
    ``os.environ.setdefault`` (an operator's explicit env wins); the
    demo never reads raw env for anything else (the settings law)."""
    assert set(run_demo_module.RETENTION_ENV) == {
        "TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD",
        "TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD",
        "TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD",
        "TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD",
        "TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD",
    }
    for name in run_demo_module.RETENTION_ENV:
        # THE HYGIENE LAW: the monkeypatch's undo owns the restore (a bare
        # os.environ write has no teardown; the direct-mutation pin
        # convicts exactly this shape).
        monkeypatch.delenv(name, raising=False)
    run_demo_module._arm_env()
    for name, value in run_demo_module.RETENTION_ENV.items():
        assert os.environ.get(name) == value
