"""THE FLAKE DRILL: a 1000-transaction batch under the 15% flake + 5%
timeout profile. ALL reach typed terminal; the retry counts are
recorded; NOTHING lost, NOTHING duplicated; the fan-out's CONCURRENCY
is measured (real overlap, not serial). The invariants run
UNCONDITIONALLY — a red under this profile is a finding, never a knob.
"""

from __future__ import annotations

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import CompletionReport
from examples.ledgerwatch.tests.support import (
    Fleet,
    child_jobs,
    journal_receipt,
    journal_rows,
    ledger_attempts,
)

pytestmark = pytest.mark.timeout(900)

#: THE ADVERSE WORLD (recorded): 15% transient flake + 5% hangs past the
#: declared timeout + 50-300 ms jitter. The ladder budget is 8 (the
#: product's recorded config; the invariants do not depend on it).
FLEET_ENV: dict[str, str] = {"LW_PROFILE": "adverse", "LW_ENRICH_CLIENT_RETRIES": "8"}

BATCH_COUNT = 1000


async def test_flake_drill_all_terminal_under_adverse_profile(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-flake-1", count=BATCH_COUNT, high_every=0))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"

    raw = await await_terminal(pool, schema, str(claim.flow_id), timeout_s=800)
    report = CompletionReport.model_validate(raw)

    # ── THE INVARIANTS (unconditional) ──────────────────────────────────
    # 1. NOTHING LOST: the typed receipt covers every transaction, none failed.
    assert report.failed == [], f"transactions died: {report.failed[:10]}"
    assert report.cleared == sorted(t.txn_id for t in batch.transactions)
    assert report.total == BATCH_COUNT
    # 2. NOTHING DUPLICATED: every id exactly once.
    assert len(report.cleared) == len(set(report.cleared))
    # 3. The fan-out's children: exactly BATCH_COUNT enrich children + BATCH_COUNT route children, all succeeded.
    enrich_jobs = await child_jobs(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    assert enrich_jobs == {"succeeded": BATCH_COUNT}, (
        f"the enrich fan-out's rows lie: {enrich_jobs}"
    )
    route_jobs = await child_jobs(pool, schema, "branch.item:%", flow_id=str(claim.flow_id))
    assert route_jobs == {"succeeded": BATCH_COUNT}, f"the route fan-out's rows lie: {route_jobs}"

    # 4. THE RETRY RECEIPT, from the EXTERNAL BOUNDARY's own journal
    # (durable, per-call): exactly ONE successful call per transaction —
    # a retry never double-succeeds — every call before it adverse, and
    # the engine's attempt ledger never shows more attempts than the
    # world made calls (the record and the world agree).
    attempts = await ledger_attempts(pool, schema, "ingest.item", flow_id=str(claim.flow_id))
    rows = await journal_rows(pool, schema, txn_prefix="B-flake-1-")
    j = journal_receipt(rows)
    assert len(j["calls_per_txn"]) == BATCH_COUNT, (
        "a transaction never called the API — or its journal row is lost"
    )
    ok_total = j["outcomes"].get("ok", 0)
    assert ok_total == BATCH_COUNT, (
        f"{ok_total} successful calls for {BATCH_COUNT} transactions — a retry double-succeeded or a txn is lost"
    )
    for txn_id, calls in j["calls_per_txn"].items():
        map_index = int(txn_id.split("-T")[-1])
        ledger_n = attempts.get(map_index, 0)
        assert ledger_n <= calls, (
            f"txn {txn_id}: {calls} external calls but {ledger_n} engine attempts — the record and the world disagree"
        )

    # 5. THE ADVERSE WORLD WAS REAL: the drill ran under flakes and
    # timeouts (a green without adverse events would be a drill of
    # nothing — the profile is seeded deterministic, so this is stable).
    outcomes = j["outcomes"]
    flaky = outcomes.get("flake", 0) + outcomes.get("hang", 0) + outcomes.get("timeout", 0)
    assert flaky > 0, "no adverse events fired — the drill's world was not injected"
    assert outcomes.get("ok", 0) == BATCH_COUNT, "every txn must land EXACTLY one successful call"

    # 6. THE CONCURRENCY IS REAL: the enrich calls overlapped (not serial).
    assert j["max_in_flight"] > 1, (
        f"the fan-out enriched SERIALLY (max in-flight {j['max_in_flight']}) — the concurrency claim is false"
    )

    # THE RECEIPT (the report's evidence): the profile, the retry
    # histogram, the measured overlap.
    from examples.ledgerwatch.config import ADVERSE_PROFILE

    attempt_histogram: dict[int, int] = {}
    for n in attempts.values():
        attempt_histogram[n] = attempt_histogram.get(n, 0) + 1
    from examples.ledgerwatch.tests.support import capture

    capture(
        "flake-drill",
        {
            "profile": {**ADVERSE_PROFILE.labeled(), "enrich_client_retries": 8},
            "batch_count": BATCH_COUNT,
            "journal": {k: v for k, v in j.items() if k != "calls_per_txn"},
            "attempt_histogram": attempt_histogram,
            "report": report.model_dump(),
        },
    )
