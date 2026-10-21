"""THE CHAOS SUITE — the flake + crash drills REPEATED (5 iterations),
the variance HONEST: every iteration's receipt lands; the invariant is
unconditional EVERY time (all typed terminals, nothing lost, nothing
duplicated), under a SIGKILL at a different point each iteration."""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import await_terminal, make_docs
from taskq.workflows import FlowRunner

ITERATIONS = 5
#: The kill lands deeper into each run every iteration (a different
#: statement window each time — the honest variance face).
KILL_DELAY_STEPS_S = [0.3, 0.8, 1.5, 2.5, 4.0]


@pytest.mark.parametrize("iteration", range(ITERATIONS))
@pytest.mark.timeout(280)
async def test_chaos_flake_and_kill(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
    iteration: int,
) -> None:
    docs: list[DocPayload] = make_docs(30, prefix=f"chaos{iteration}")
    delivery_id = f"chaos-{iteration}-{secrets.token_hex(4)}"

    flow_ids = await deliver_batch(pool, schema, DeliveryBatch(delivery_id=delivery_id, docs=docs))
    assert len(flow_ids) == 1
    flow_id = flow_ids[0]

    # THE CRASH: a worker dies KILL_DELAY into the run (the fan-out is
    # live by then); the replacement worker finishes the story.
    worker = await workers.spawn_ready(pool, tag=f"chaos-{iteration}-a", max_concurrency=8)
    await asyncio_sleep(KILL_DELAY_STEPS_S[iteration % len(KILL_DELAY_STEPS_S)])
    killed = worker.alive()
    worker.kill9()
    await workers.spawn_ready(pool, tag=f"chaos-{iteration}-b", max_concurrency=8)

    started = time.monotonic()
    status = await await_terminal(pool, schema, flow_id, timeout_s=240)
    wall_s = time.monotonic() - started

    # THE INVARIANT, UNCONDITIONAL — every iteration, no exceptions:
    assert status == "succeeded", f"iteration {iteration}: the run terminal-failed: {status}"

    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digest = ShelfDigest.model_validate(await runner.result(flow_id))
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert sorted(everything) == sorted(d.doc_id for d in docs), (
        f"iteration {iteration}: lost or doubled documents"
    )
    assert len(set(everything)) == len(docs)

    summary = await store.api_call_summary(pool, schema)
    mine = {k: v for k, v in summary.items() if k.startswith(f"chaos{iteration}-")}
    total_attempts = sum(v.get("start", 0) for v in mine.values())
    flakes = sum(v.get("flake", 0) for v in mine.values())
    timeouts = sum(v.get("timeout", 0) for v in mine.values())
    re_successes = sum(1 for v in mine.values() if v.get("success", 0) > 1)

    run_receipt(
        f"chaos-iteration-{iteration}",
        {
            "iteration": iteration,
            "flow_id": flow_id,
            "worker_killed_alive": killed,
            "kill_delay_s": KILL_DELAY_STEPS_S[iteration % len(KILL_DELAY_STEPS_S)],
            "wall_seconds": round(wall_s, 1),
            "digest_total_docs": digest.total_docs,
            "child_flows_spawned": digest.child_flows_spawned,
            "failed_children": digest.failed_children,
            "extraction_attempts_total": total_attempts,
            "flakes_injected": flakes,
            "timeouts_injected": timeouts,
            "chunks_re_extracted_after_kill": re_successes,
        },
    )


def asyncio_sleep(s: float) -> Any:
    import asyncio

    return asyncio.sleep(s)
