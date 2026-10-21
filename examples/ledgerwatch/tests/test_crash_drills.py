"""THE CRASH DRILLS: SIGKILL the worker mid-enrich-fan-out AND
mid-approval-wait. The restart resumes; NOTHING lost, NOTHING
duplicated; the approval hold survives the death."""

from __future__ import annotations

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import ApprovedTxn, CompletionReport
from examples.ledgerwatch.tests.support import (
    Fleet,
    child_jobs,
    journal_receipt,
    journal_rows,
    ledger_attempts,
    review_run_ids,
    wait_for,
)
from taskq.workflows import HitlClient

pytestmark = pytest.mark.timeout(600)

FLEET_ENV: dict[str, str] = {"LW_PROFILE": "adverse"}


async def test_sigkill_mid_enrich_fan_out_resumes_with_nothing_lost(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-crash-1", count=300, high_every=0))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"

    # Wait until the fan-out is LIVE: calls started, some in flight.
    async def _fan_out_live() -> int | None:
        rows = await journal_rows(pool, schema, txn_prefix="B-crash-1-")
        j = journal_receipt(rows)
        inflight = j["total_calls"] - journal_finished(rows)
        if j["total_calls"] >= 80 and inflight >= 10:
            return j["total_calls"]
        return None

    calls_at_kill = await wait_for(
        _fan_out_live, timeout_s=120, interval=0.1, message="the fan-out never went live"
    )

    # THE CRASH: SIGKILL every worker in the fleet.
    await fleet.kill_all()

    # The estate's rows survive the process: the journal's calls are durable.
    rows_after = await journal_rows(pool, schema, txn_prefix="B-crash-1-")
    assert len(rows_after) >= calls_at_kill, (
        "the crash ate the external API's journal — the world cannot lose calls"
    )

    # THE RESTART: a fresh worker claims the re-pended work.
    await fleet.start(1)
    raw = await await_terminal(pool, schema, str(claim.flow_id), timeout_s=600)
    report = CompletionReport.model_validate(raw)

    # ── THE INVARIANTS (unconditional) ──────────────────────────────────
    assert report.failed == [], f"transactions died in the crash: {report.failed[:10]}"
    assert report.cleared == sorted(t.txn_id for t in batch.transactions)
    assert len(report.cleared) == len(set(report.cleared)), "a transaction cleared twice"
    enrich_jobs = await child_jobs(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    assert enrich_jobs == {"succeeded": 300}, (
        f"the fan-out's rows lie after the crash: {enrich_jobs}"
    )

    # THE RESUME PROOF: the crash interrupted real work — the engine's
    # ladder carried at least one child PAST attempt 1 (the reclaim
    # re-pended it; the restart re-ran it alone).
    attempts = await ledger_attempts(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    resumed = sum(1 for n in attempts.values() if n > 1)
    assert resumed >= 1, (
        "no child shows a resumed attempt — the SIGKILL interrupted nothing "
        "(the drill killed a quiescent worker, proving nothing)"
    )

    # THE JOURNAL'S HONESTY: every transaction landed EXACTLY ONE
    # applied verdict — a successful call — and a SECOND successful
    # call (the at-least-once boundary: a body killed mid-call re-runs
    # it; the docs state the boundary, the framework guarantees
    # exactly-once only for DB-local effects) appears only for children
    # the crash actually resumed. The verdicts are identical either way
    # (the deterministic mock) — the OUTCOME never doubles.
    j = journal_receipt(await journal_rows(pool, schema, txn_prefix="B-crash-1-"))
    oks = j["oks_per_txn"]
    assert len(oks) == 300, "a transaction never got a successful call"
    doubled = {t: n for t, n in oks.items() if n > 1}
    assert all(n <= 2 for n in oks.values()), f"a txn enriched >2x: {doubled}"
    for t in doubled:
        map_index = int(t.split("-T")[-1])
        assert attempts.get(map_index, 0) > 1, (
            f"txn {t} enriched twice but shows no resumed attempt — a double-effect without a crash"
        )
    assert "inflight" in j["outcomes"], (
        "the crash left no started-but-unfinished call — the kill landed after the work"
    )

    from examples.ledgerwatch.config import ADVERSE_PROFILE
    from examples.ledgerwatch.tests.support import capture

    capture(
        "crash-drill-fanout",
        {
            "profile": ADVERSE_PROFILE.labeled(),
            "calls_at_kill": calls_at_kill,
            "calls_after": len(rows_after),
            "resumed_children": resumed,
            "journal": {k: v for k, v in j.items() if k != "calls_per_txn"},
        },
    )


async def test_sigkill_mid_approval_wait_hold_survives(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-crash-2", count=2, high_every=1))
    claim = await submit_batch(pool, schema, batch)
    await await_terminal(pool, schema, str(claim.flow_id), timeout_s=120)

    async def _two_holds() -> list[str] | None:
        ids = await review_run_ids(pool, schema, batch_id=batch.batch_id)
        if len(ids) < 2:
            return None
        client = HitlClient(pool, schema=schema)
        holds = await client.list(ids[0]) + await client.list(ids[1])
        return [str(h.hold_id) for h in holds] if len(holds) == 2 else None

    holds_before = await wait_for(_two_holds, timeout_s=60, message="the review runs never held")
    ids = await review_run_ids(pool, schema, batch_id=batch.batch_id)

    # THE CRASH, mid-hold.
    await fleet.kill_all()

    # THE HOLD SURVIVES THE DEATH: the hold rows are Postgres rows; the
    # new worker's sweep never touches a pending hold. (Scoped to THESE
    # two review runs — the estate may carry other runs' holds.)
    held = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_signals'  # noqa: S608  # Why: the schema identifier only.
        " WHERE status = 'held' AND workflow_id = ANY($1::uuid[])",
        list(ids),
    )
    assert held == 2, f"the hold rows did not survive the SIGKILL: {held}"

    await fleet.start(1)

    # The operator resolves BOTH holds; the runs continue to typed verdicts.
    client = HitlClient(pool, schema=schema)
    for rid in ids:
        (hold,) = await client.list(rid)
        assert str(hold.hold_id) in holds_before, (
            "the restart re-minted the hold — the identity did not survive"
        )
        result = await client.resolve(hold.hold_id, {"approved": True, "reviewer": "op-crash"})
        assert result.status == "delivered"
        raw = await await_terminal(pool, schema, rid, workflow="txn_review", timeout_s=60)
        verdict = ApprovedTxn.model_validate(raw)
        assert verdict.reviewer == "op-crash"


def journal_finished(rows: list[asyncpg.Record]) -> int:
    return sum(1 for r in rows if r["finished_at"] is not None)
