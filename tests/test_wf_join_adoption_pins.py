# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The fork's retained join-adoption path (the cross-release compatibility
surface), and the parser that reads the stamp it exists for.

Before #745 the outer fork listed a nested map's derived join among its own
join's consumers, so an older release's fire put a binding for that join on
the outbox and the drain pre-created the row under ``wf:<step>``. After #745
no binding names a ``map_join``, so a persisted stamp that still does is the
only remaining producer — an in-flight run's row, written by a release that
predates the fix.

``insert_fork`` therefore keeps an adoption branch, and it is the whole
in-flight compatibility path. Two pinned laws:

``test_insert_fork_skips_an_already_run_nested_join_row_instead_of_adopting_it``
    The probe reads only a PENDING row. A row a worker already finished (or
    is running) must never be adopted: the adopted children's decrements are
    gated on ``jobs.status = 'pending'``, so the counter never drains, the
    nested join never fires, the post-nested-map consumer keeps its reserved
    dep and the run rests ``running`` with every child succeeded. With the
    guard the fork takes its normal insert path and the run completes with
    the TRUE children. The skipped residue is reported
    (``wf-fork-join-adoption-skipped``) — after the guard it is invisible to
    all three ``docs/guides/upgrading.md`` detectors on a two-level shape,
    because a never-adopted row keeps ``deps_pending = 0``.

``test_the_persisted_consumer_parser_raises_on_a_malformed_entry``
    The parser that reads that stamp fails loudly on a malformed entry —
    the three required keys raise ``KeyError``, the structured sub-docs
    raise ``ValueError`` — and degrades ONLY the declared deadline. It is a
    cross-release parse: the docstring says what the code does, and the
    raise is the leader sweep's loud refusal, not a promised silent
    degrade.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

import taskq.workflows._sweep as sweep_module
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from taskq.workflows._types import ConsumerBinding, _consumer_binding_doc, _consumer_bindings

pytestmark = pytest.mark.integration


class Seed(BaseModel):
    n: int


async def seeds(ctx: StepContext) -> list[Seed]:
    return [Seed(n=i) for i in range(4)]


async def expand(ctx: StepContext, seed: Seed) -> list[str]:
    return [f"s{seed.n}-{k}" for k in range(3)]


async def ident(ctx: StepContext, x: object) -> object:
    return x


async def count(ctx: StepContext, xs: list[object]) -> int:
    return len(xs)


def _app() -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow("adoption_of_a_finished_row")
    def adoption_of_a_finished_row() -> Promise[object]:
        first = map_source(step(seeds, key="seeds"), expand)
        return build(step(count, map_source(first, ident), key="final"))

    return app


async def _one_row(conn: asyncpg.Connection, schema: str, flow_id: Any, key: str) -> dict[str, Any]:
    found = await conn.fetch(
        f'SELECT id, step_key, status, deps_pending, idempotency_key FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        key,
    )
    assert len(found) == 1, f"expected one {key!r} row; got {[dict(r) for r in found]}"
    return dict(found[0])


async def test_insert_fork_skips_an_already_run_nested_join_row_instead_of_adopting_it(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The interleaving a rolling deploy can produce: the drain pre-creates
    the nested join from a pre-#745 binding and a worker claims and finishes
    it before the outer join's own fork probes.

    The drain row is marked ``succeeded`` directly rather than driven through
    a second worker: one process cannot race itself — the in-process driver
    claims a run's claimable rows in id order, and the outer join's id is
    always minted before the drained row's. The row state written here is the
    state the fleet produces.

    The law: the post-nested-map step must still get its result. A fork that
    adopted the finished row would keep ``deps_pending = 4`` on it (its own
    children's decrements are gated on ``jobs.status = 'pending'``, which the
    finished row is not), so the nested join would never fire and ``final``
    would keep the dep the static insert reserved for it. The guarded probe
    refuses the row, the fork inserts its own, and the run completes with the
    true children.
    """
    app = _app()
    runner = FlowRunner(app.get("adoption_of_a_finished_row"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    real_drain = sweep_module.drain_outbox

    async def _muted_drain(*_a: object, **_k: object) -> int:
        return 0

    # The leader's drain lags the dispatch (sweep_interval): hold it back so
    # the ordering below is the test's, not the tick's.
    monkeypatch.setattr(sweep_module, "drain_outbox", _muted_drain)

    # One tick: ``seeds`` finalizes and its fork writes ``seeds.item`` plus
    # ``seeds.join`` — whose consumers are empty (the #745 fix).
    await runner.tick(flow_id)

    # Re-stamp the outer join with the binding an OLD release's fork wrote, so
    # the fire puts the nested join on the outbox. Since #745 this is the only
    # way a ``map_join`` binding exists at all.
    decl = runner.compiled.nodes["seeds.join.join"]
    old_binding = _consumer_binding_doc(
        ConsumerBinding(
            step_key="seeds.join.join",
            actor=decl.actor,
            queue=decl.queue,
            payload={},
            failure_policy=decl.on_failure,
            start_to_close=decl.start_to_close,
        )
    )
    stamped = await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs
            SET metadata = jsonb_set(metadata, '{{consumers}}', $2::jsonb, true)
            WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'seeds.join'""",
        flow_id,
        json.dumps([old_binding]),
    )
    assert stamped == "UPDATE 1", stamped

    # Drive the outer map's children to terminal so the join fires, and stop
    # at the instant it is claimable but not yet forked.
    for _ in range(20):
        outer = await _one_row(wf_conn, wf_schema, flow_id, "seeds.join")
        if outer["status"] == "pending" and outer["deps_pending"] == 0:
            break
        await runner.tick(flow_id)
    outer = await _one_row(wf_conn, wf_schema, flow_id, "seeds.join")
    assert outer["status"] == "pending" and outer["deps_pending"] == 0, outer
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'seeds.join.join'",
            flow_id,
        )
        == 0
    ), "the nested join must not exist before its own source forks"

    # The leader's drain lands: the pre-fix binding pre-creates the nested join
    # (deps 0, no join-wait marker — claimable by anyone).
    await real_drain(wf_pool, runner.wsql)
    drained = await _one_row(wf_conn, wf_schema, flow_id, "seeds.join.join")
    assert drained["idempotency_key"] == "wf:seeds.join.join", drained
    assert drained["deps_pending"] == 0 and drained["status"] == "pending", drained

    # A worker claims it and finishes it before the outer join's fork probes:
    # the join body is the default packer over no parents, so it succeeds with
    # the empty collect — the pre-fix "ran on an empty join".
    finished = await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs
            SET status = 'succeeded', attempt = 1, result = '{{"value": []}}'::jsonb
            WHERE id = $1 AND status = 'pending'""",
        drained["id"],
    )
    assert finished == "UPDATE 1", finished

    monkeypatch.setattr(sweep_module, "drain_outbox", real_drain)
    with structlog.testing.capture_logs() as logs:
        verdict = await runner.drive(flow_id, max_ticks=120)

    observed = {
        "nested_join_rows": [
            dict(r)
            for r in await wf_conn.fetch(
                f'SELECT id, status, deps_pending, idempotency_key FROM "{wf_schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'seeds.join.join' "
                "ORDER BY id",
                flow_id,
            )
        ],
        "post_nested_map_step": [
            dict(r)
            for r in await wf_conn.fetch(
                f'SELECT status, deps_pending FROM "{wf_schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'final'",
                flow_id,
            )
        ],
    }
    assert verdict == "terminal", (
        "the fork adopted a nested join row that had already run, so the nested join "
        "never fired, the post-nested-map step kept its reserved dep, and the run never "
        "terminalised — observed " + json.dumps(observed)
    )
    result = await runner.result(flow_id)
    assert result == 4, (result, observed)

    # The fork wrote its OWN row (parent-scoped key) with the true children…
    fork_row = next(
        (r for r in observed["nested_join_rows"] if str(r["id"]) != str(drained["id"])), None
    )
    assert fork_row is not None, observed
    assert fork_row["idempotency_key"] == f"wf:{flow_id}:seeds.join:seeds.join.join", fork_row
    assert fork_row["status"] == "succeeded" and fork_row["deps_pending"] == 0, fork_row
    # …and left the already-run row untouched: skipped, never adopted.
    residue = next(
        (r for r in observed["nested_join_rows"] if str(r["id"]) == str(drained["id"])), None
    )
    assert residue == {
        "id": drained["id"],
        "status": "succeeded",
        "deps_pending": 0,
        "idempotency_key": "wf:seeds.join.join",
    }, observed
    assert observed["post_nested_map_step"] == [{"status": "succeeded", "deps_pending": 0}], (
        observed
    )

    # The refusal is reported: the residue is invisible to the upgrade
    # detectors once it is never adopted, so the system's own records are the
    # only place an operator can find it.
    skipped = [entry for entry in logs if entry.get("event") == "wf-fork-join-adoption-skipped"]
    assert len(skipped) == 1, logs
    assert skipped[0]["flow_id"] == str(flow_id)
    assert skipped[0]["step_key"] == "seeds.join.join"
    assert skipped[0]["existing_job_id"] == str(drained["id"])
    assert skipped[0]["existing_status"] == "succeeded"
    assert skipped[0]["log_level"] == "warning"


def test_the_persisted_consumer_parser_raises_on_a_malformed_entry() -> None:
    """The persisted ``consumers`` stamp's parser validates rather than
    degrading — except the declared deadline.

    The three required keys are read directly: a stamp that lacks one raises
    ``KeyError``. ``rate_limits``/``reservations`` and ``retry`` are
    structured sub-docs whose own readers raise ``ValueError``. Only
    ``start_to_close_s`` degrades to ``None`` (it is a duration hint, not a
    correctness input). The parser is a cross-release boundary — an older
    release can be the only writer of a ``map_join``-naming stamp — so this
    is the contract the docstring must state, and it is pinned here because
    the leader's re-derive arm calls it inside the sweep."""
    # One missing required key — the shape a partially written / hand-edited
    # stamp produces.
    with pytest.raises(KeyError):
        _consumer_bindings([{"actor": "a", "queue": "q"}])
    # A wrong-typed required key raises too (never a silent coercion).
    with pytest.raises(ValueError):
        _consumer_bindings([{"step_key": 1, "actor": "a", "queue": "q"}])
    # A present-but-malformed structured field raises (both readers).
    with pytest.raises(ValueError):
        _consumer_bindings([{"step_key": "k", "actor": "a", "queue": "q", "rate_limits": "x"}])
    with pytest.raises(ValueError):
        _consumer_bindings([{"step_key": "k", "actor": "a", "queue": "q", "retry": "x"}])
    # Only the declared deadline actually degrades to None.
    (binding,) = _consumer_bindings(
        [{"step_key": "k", "actor": "a", "queue": "q", "start_to_close_s": "nope"}]
    )
    assert binding.start_to_close is None
