"""The scenario grounds' companion test: one test per scenario, each
running the scenario script and asserting the row-truth of the printed
evidence — the statuses, counts and orderings the rows must answer for.

The scenarios need a real Postgres (and a real Redis for the scenarios
that start a worker): point ``TASKQ_PG_DSN`` / ``TASKQ_REDIS_URL`` at
them (the defaults match the repository's compose stack) and set
``TASKQ_SCENARIO_GROUNDS=1`` to opt in:

    TASKQ_SCENARIO_GROUNDS=1 nice -n 5 uv run pytest tests/test_scenario_grounds.py

Each scenario builds its own schema from scratch, so the tests are
serial by nature and re-run cleanly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PG_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("TASKQ_SCENARIO_GROUNDS") != "1",
        reason="the scenario grounds need a real Postgres/Redis: set TASKQ_SCENARIO_GROUNDS=1 to opt in",
    ),
]


def _run_scenario(module: str) -> dict[str, dict[str, Any]]:
    """Run one scenario script and parse its evidence lines."""
    env = {
        **os.environ,
        "TASKQ_PG_DSN": os.environ.get("TASKQ_PG_DSN", DEFAULT_PG_DSN),
        "TASKQ_REDIS_URL": os.environ.get("TASKQ_REDIS_URL", DEFAULT_REDIS_URL),
        # Forced, never defaulted: an ambient production label would
        # crash the scenario's page face (the admin's UI mounts without
        # an auth dependency only under the dev label). The page face
        # is dev-only by design.
        "TASKQ_ENVIRONMENT": "development",
    }
    proc = subprocess.run(  # noqa: S603  # Why: fixed argv, this interpreter, the scenario module's own name — no caller input reaches the command line.
        [sys.executable, "-m", f"examples.scenario_grounds.{module}"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        timeout=280,
    )
    assert proc.returncode == 0, (
        f"the scenario {module} did not finish green "
        f"(exit {proc.returncode}):\n{proc.stdout.decode()}\n{proc.stderr.decode()}"
    )
    evidence: dict[str, dict[str, Any]] = {}
    for line in proc.stdout.decode().splitlines():
        if line.startswith("EVIDENCE "):
            payload = json.loads(line[len("EVIDENCE ") :])
            evidence[str(payload["check"])] = payload
    return evidence


def test_s1_the_fan_out_with_the_approval_hold() -> None:
    evidence = _run_scenario("s1_fanout_hold")
    held = evidence["hold_held"]
    assert held["pending"] == 1
    assert held["signal"] == "Approval"
    lanes = evidence["queues_claimed"]["lanes"]
    assert lanes["grounds-fast"] == {"rows": 3, "succeeded": 3}
    assert lanes["grounds-heavy"] == {"rows": 2, "succeeded": 2}
    resolved = evidence["hold_resolved"]
    assert resolved["before"] == "held"
    assert resolved["after"] == "delivered"
    assert resolved["verdict"] == "delivered"
    terminal = evidence["terminal"]
    assert terminal["root"] == "succeeded"
    assert "published" in terminal["result"]


def test_s2_the_three_tenant_priority_split() -> None:
    evidence = _run_scenario("s2_priority_split")
    order = evidence["claim_order"]
    # The claim order, read off the rows: every paying row before any
    # standard row, every standard row before any free-tier row.
    assert order["encoding"] == "30x50,10x50,0x50"
    assert order["tenants"] == {
        "alpine-pay": "30x50",
        "baseline": "10x50",
        "freemium": "0x50",
    }
    assert evidence["priority_columns"]["counts"] == {"30": 50, "10": 50, "0": 50}
    completed = evidence["all_completed"]
    assert completed["runs"] == 150
    assert completed["succeeded"] == 150
    assert completed["failed"] == 0
    assert completed["cancelled"] == 0


def test_s3_the_max_concurrent_damper() -> None:
    evidence = _run_scenario("s3_max_concurrent")
    cap = evidence["never_over_cap"]
    assert cap["cap"] == 2
    assert cap["peak_in_flight"] <= 2, "more than the cap ran at once"
    assert evidence["cap_reached"]["reached"] is True, "the cap never bound"
    completed = evidence["all_completed"]
    assert completed["runs"] == 10
    assert completed["succeeded"] == 10


def test_s4_the_reservation_guard() -> None:
    evidence = _run_scenario("s4_reservation_guard")
    waiting = evidence["step_waits"]
    # The wait's face: the row parked (pending or its snoozed form),
    # the attempt unspent, and the denial's own stamp on the row —
    # the reason and the denied bucket, read off the row itself.
    assert waiting["status"] in ("pending", "scheduled")
    assert int(waiting["attempt"]) <= 0
    assert waiting["released_reason"] == "flow-rate-limit-denied"
    assert waiting["released_bucket"] == "grounds_scene_slots"
    assert evidence["never_failed_during_hold"]["error_class"] is None
    seats = evidence["seats_held_by_holder"]
    # The occupancy and the bucket are the reservation table's own read
    # and the row's own stamp — the pin answers for the rows, not for
    # the scenario's constants.
    assert int(seats["held"]) == 2
    assert seats["bucket"] == "grounds_scene_slots"
    proceeds = evidence["step_proceeds"]
    assert proceeds["status"] == "succeeded"
    released = evidence["seats_released"]
    assert int(released["held"]) == 0
    flow = evidence["flow_completed"]
    assert flow["root"] == "succeeded"
    assert flow["step"] == "succeeded"


def test_s5_retention_live() -> None:
    evidence = _run_scenario("s5_retention_live")
    before = evidence["rows_before"]["counts"]
    after = evidence["rows_after"]["counts"]
    assert set(before) == set(after)
    for table in before:
        # Every history table held the aged runs' rows before the sweep
        # and none after.
        assert before[table]["aged"] > 0, f"{table} had no aged rows to prune"
        assert after[table]["aged"] == 0, f"{table} kept the aged runs' rows"
        # The live run's rows survived, table for table.
        assert after[table]["live"] == before[table]["live"], (
            f"{table} touched the live run's history"
        )
    assert evidence["live_run_untouched"]["surviving_rows"] > 0
    assert evidence["prune_record"]["arms_reported"] == 5, (
        "the sweep's own prune record did not name all five arms"
    )


def test_s6_the_retry_ladder() -> None:
    evidence = _run_scenario("s6_retry_ladder")
    assert evidence["attempt_ledger"]["attempts"] == [
        "1:failed",
        "2:failed",
        "3:succeeded",
    ]
    # The curve, read off the re-pend's own stamped provenance (the
    # re-pend statement writes the applied backoff and the due instant
    # into the row's metadata, atomically with the re-pend — no
    # polling, no reconstruction): each attempt's due-minus-at within
    # the curve's margins, the second step the doubling wider. A
    # flattened curve stamps ~0.05s at the second step and reds
    # deterministically.
    steps = evidence["backoff_steps"]
    assert 0.05 - 0.02 <= float(steps["step_1"]) <= 0.05 + 0.25, steps
    assert 0.10 - 0.02 <= float(steps["step_2"]) <= 0.10 + 0.25, steps
    assert evidence["curve_doubled"]["widening_s"] >= 0.05 - 0.02, "the backoff did not double"
    assert evidence["claim_honored_due"]["honored"] is True
    success = evidence["success"]
    assert success["outcome"] == "terminal"
    assert "attempt': 3" in str(success["result"]) or "attempt: 3" in str(success["result"])


def test_s7_the_degraded_faces() -> None:
    evidence = _run_scenario("s7_degraded_faces")
    refusal = evidence["schema_refusal"]
    assert refusal["refused"] is True
    assert refusal["error_class"] == "SchemaNotMigratedError"
    page = evidence["page_notice"]
    assert page["status"] == 200
    assert page["notice_shown"] is True
    parked = evidence["stale_row_parked"]
    assert parked["reason"] == "workflow-body-unresolvable"
    assert parked["status"] in ("pending", "scheduled")
    healed = evidence["stale_run_healed"]
    assert healed["root"] == "succeeded"
    queue = evidence["queue_refusal"]
    assert int(queue["exit_code"]) != 0
    assert queue["refused"] is True
    assert queue["names_queue"] is True
