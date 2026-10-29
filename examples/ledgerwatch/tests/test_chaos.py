"""THE CHAOS SUITE: the flake drill and the SIGKILL crash drill,
REPEATEDLY (5 iterations), the variance recorded honestly. Every
iteration's invariants are the drills' own, UNCONDITIONAL: all
transactions reach typed terminal, nothing lost, nothing duplicated,
the resume proven. A red here is a finding, never a knob."""

from __future__ import annotations

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.config import ADVERSE_PROFILE
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import CompletionReport
from examples.ledgerwatch.tests.support import (
    Fleet,
    capture_append,
    child_jobs,
    journal_receipt,
    journal_rows,
    ledger_attempts,
    wait_for,
)

pytestmark = pytest.mark.timeout(900)

FLEET_ENV: dict[str, str] = {"LW_PROFILE": "adverse", "LW_ENRICH_CLIENT_RETRIES": "8"}

ITERATIONS = 5


async def _run_flake_drill(pool: asyncpg.Pool, schema: str, iteration: int) -> dict[str, object]:
    batch_id = f"B-chaos-flake-{iteration}"
    batch = build_batch(BatchSpec(batch_id=batch_id, count=1000, high_every=0))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"
    raw = await await_terminal(pool, schema, str(claim.flow_id), timeout_s=800)
    report = CompletionReport.model_validate(raw)

    # THE INVARIANTS (unconditional — every iteration, no exceptions).
    assert report.failed == [], f"iteration {iteration}: transactions died: {report.failed[:10]}"
    assert report.cleared == sorted(t.txn_id for t in batch.transactions)
    assert len(report.cleared) == len(set(report.cleared)), "a transaction cleared twice"
    enrich_jobs = await child_jobs(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    assert enrich_jobs == {"succeeded": 1000}, f"iteration {iteration}: {enrich_jobs}"

    attempts = await ledger_attempts(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    j = journal_receipt(await journal_rows(pool, schema, txn_prefix=batch_id))
    assert len(j["calls_per_txn"]) == 1000
    assert j["outcomes"].get("ok", 0) == 1000, (
        f"iteration {iteration}: {j['outcomes']} — a retry double-succeeded or a txn is lost"
    )
    for txn_id, calls in j["calls_per_txn"].items():
        assert attempts.get(int(txn_id.split("-T")[-1]), 0) <= calls

    return {
        "iteration": iteration,
        "drill": "flake-1000",
        "journal": {k: v for k, v in j.items() if k != "calls_per_txn"},
        "engine_attempt_histogram": _histogram(attempts),
    }


async def _run_crash_drill(
    pool: asyncpg.Pool, schema: str, fleet: Fleet, iteration: int
) -> dict[str, object]:
    batch_id = f"B-chaos-crash-{iteration}"
    batch = build_batch(BatchSpec(batch_id=batch_id, count=300, high_every=0))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"

    async def _live() -> int | None:
        j = journal_receipt(await journal_rows(pool, schema, txn_prefix=batch_id))
        if j["total_calls"] >= 60:
            return j["total_calls"]
        return None

    calls_at_kill = await wait_for(
        _live,
        timeout_s=180,
        interval=0.1,
        message=f"iteration {iteration}: the fan-out never went live",
    )

    # THE CRASH: the fleet dies mid-fan-out.
    await fleet.kill_all()

    # THE RESTART: a fresh worker resumes.
    await fleet.start(1)
    raw = await await_terminal(pool, schema, str(claim.flow_id), timeout_s=600)
    report = CompletionReport.model_validate(raw)

    assert report.failed == [], (
        f"iteration {iteration}: transactions died in the crash: {report.failed[:10]}"
    )
    assert report.cleared == sorted(t.txn_id for t in batch.transactions)
    assert len(report.cleared) == len(set(report.cleared)), "a transaction cleared twice"
    enrich_jobs = await child_jobs(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    assert enrich_jobs == {"succeeded": 300}, f"iteration {iteration}: {enrich_jobs}"

    attempts = await ledger_attempts(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    resumed = sum(1 for n in attempts.values() if n > 1)
    assert resumed >= 1, (
        f"iteration {iteration}: no child resumed — the SIGKILL interrupted nothing"
    )
    j = journal_receipt(await journal_rows(pool, schema, txn_prefix=batch_id))
    oks = j["oks_per_txn"]
    assert len(oks) == 300, f"iteration {iteration}: a txn never landed a successful call"
    assert all(n <= 2 for n in oks.values()), "a txn enriched >2x"
    doubled = {t: n for t, n in oks.items() if n > 1}
    for t in doubled:
        assert attempts.get(int(t.split("-T")[-1]), 0) > 1, (
            f"iteration {iteration}: txn {t} enriched twice with no resumed attempt"
        )

    return {
        "iteration": iteration,
        "drill": "sigkill-mid-fanout-300",
        "calls_at_kill": calls_at_kill,
        "resumed_children": resumed,
        "double_enriched_children": len(doubled),
        "journal_outcomes": j["outcomes"],
        "engine_attempt_histogram": _histogram(attempts),
    }


def _histogram(attempts: dict[int, int]) -> dict[int, int]:
    out: dict[int, int] = {}
    for n in attempts.values():
        out[n] = out.get(n, 0) + 1
    return out


@pytest.mark.parametrize("iteration", range(ITERATIONS))
async def test_chaos_iteration(
    iteration: int, fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    """One chaos iteration: the flake drill + the crash drill, back to
    back, under the recorded adverse profile."""
    _ = fleet  # the fleet is up (the module's env: the adverse profile)
    flake_receipt = await _run_flake_drill(pool, schema, iteration)
    crash_receipt = await _run_crash_drill(pool, schema, fleet, iteration)
    receipt = {"profile": ADVERSE_PROFILE.labeled(), "flake": flake_receipt, "crash": crash_receipt}
    capture_append("chaos-drill", receipt)
