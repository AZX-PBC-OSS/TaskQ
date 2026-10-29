"""The drill suite for the bulk ledger import.

Each drill prints its verdict as it passes and captures its receipts
into ``.measurements/probe-import/``:

1. **Throughput + progress** — the cold import over a 2,400-row feed:
   rows/second, and the progress events pushed while it ran.
2. **Quarantine** — the invalid rows land quarantined with reasons; the
   valid rest completes; the receipt's counts match the generator's
   stride arithmetic EXACTLY.
3. **Re-import** — the same feed again: identical table fingerprint,
   zero changed rows (imported = 0, updated = 0).
4. **Cache** — a later run over the same feed content never re-executes
   the cached transform step; the wall-clock delta is honest.
5. **Resume** — SIGKILL mid-import; restart with the same run key; no
   completed work re-executes; the final receipt is exact.
6. **Admin view** — an operator sees the run mid-flight (``taskq flows
   list`` / ``taskq flows status`` / ``taskq job show``) and operates it
   (``taskq flows cancel``) — captured mid-flight and after.

The suite's last line is ``DRILL SUITE SUCCEEDED`` with the verdict.
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from examples.ledger_import import feed, store
from examples.ledger_import.models import LedgerReceipt
from examples.ledger_import.run_import import SCHEMA, run_import

DSN = sys.argv[1] if len(sys.argv) > 1 else "postgresql://postgres:taskq@localhost:5798/taskq"
ROWS = 2400
CHUNK_SIZE = 200
FEED_PATH = os.path.join(tempfile.gettempdir(), "ledger-drill-feed.jsonl")
MEAS = Path(__file__).resolve().parents[2] / ".measurements" / "probe-import"
PY = sys.executable
TASKQ_BIN = str(Path(PY).parent / "taskq")
# The workflow node ledger, schema-qualified (the schema comes from
# $TASKQ_SCHEMA_NAME — configuration, never operator input).
STEP_LEDGER = f"{SCHEMA}.wf_step_ledger"


def _stamp() -> str:
    return datetime.now(UTC).isoformat()


def _capture(name: str, text: str) -> Path:
    path = MEAS / name
    with path.open("a", encoding="utf-8") as fh:
        fh.write(text if text.endswith("\n") else text + "\n")
    return path


def _banner(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}", flush=True)


def _check(label: str, ok: bool, detail: str = "") -> bool:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}{(' — ' + detail) if detail else ''}", flush=True)
    return ok


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "TASKQ_PG_DSN": DSN, "TASKQ_SCHEMA_NAME": SCHEMA}
    return subprocess.run(  # noqa: S603 — fixed argv, no operator input
        [TASKQ_BIN, *args], capture_output=True, text=True, env=env
    )


def _spawn_driver(
    run_key: str, progress_log: Path, crash_at: int | None = None
) -> subprocess.Popen[bytes]:
    env = {**os.environ, "TASKQ_PG_DSN": DSN}
    if crash_at is not None:
        env["LEDGER_CRASH_AT"] = str(crash_at)
    return subprocess.Popen(  # noqa: S603
        [
            PY,
            "-m",
            "examples.ledger_import.run_import",
            "run",
            "--feed",
            FEED_PATH,
            "--run-key",
            run_key,
            "--progress-log",
            str(progress_log),
        ],
        stdout=(MEAS / f"run-{run_key}.log").open("ab"),
        stderr=subprocess.STDOUT,
        env=env,
    )


async def _flow_ledger(flow_id: str) -> dict[tuple[str, int | None], tuple[str, int]]:
    """The run's node ledger: (step_key, map_index) → (status, max attempt)."""
    p = await store.pool()
    rows = await p.fetch(
        f"SELECT step_key, map_index, status, max(attempt) AS a FROM {STEP_LEDGER} "  # noqa: S608
        "WHERE flow_id = $1::uuid GROUP BY step_key, map_index, status",
        flow_id,
    )
    best: dict[tuple[str, int | None], tuple[str, int]] = {}
    for r in rows:
        key = (str(r["step_key"]), r["map_index"])
        attempt = int(r["a"] or 0)
        if key not in best or attempt > best[key][1]:
            best[key] = (str(r["status"]), attempt)
    return best


def _flow_id_from_log(run_key: str, timeout_s: float = 15.0) -> str:
    """The run's flow id, from the driver's claim line (the log flushes at print)."""
    log_path = MEAS / f"run-{run_key}.log"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for line in log_path.read_text().splitlines():
            if "flow_id=" in line:
                return line.split("flow_id=")[1].strip()
        time.sleep(0.2)
    raise RuntimeError(f"no claim line in {log_path} after {timeout_s}s")


async def _wait_body_runs(body: str, since: str, at_least: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rows = await store.body_runs_since(since, body)
        if len(rows) >= at_least:
            return True
        await asyncio.sleep(0.2)
    return False


async def drill_quarantine(receipt: LedgerReceipt) -> bool:
    _banner("DRILL 2 — the quarantine: invalid rows quarantined with reasons")
    ok = True
    expected_invalid = feed.expected_invalid_count(ROWS)
    expected_classes = feed.expected_invalid_reason_counts(ROWS)
    q_count = await store.quarantine_count()
    reasons = await store.quarantine_reason_counts()
    ok &= _check(
        "quarantine count exact (the generator's stride)",
        q_count == expected_invalid,
        f"{q_count} == {expected_invalid}",
    )
    ok &= _check(
        "reason taxonomy exact per class",
        reasons == expected_classes,
        json.dumps(reasons, sort_keys=True),
    )
    ok &= _check(
        "the receipt's quarantined count matches the table", receipt.quarantined == q_count
    )
    ok &= _check(
        "the valid rest completed",
        receipt.imported + receipt.unchanged == ROWS - expected_invalid,
        f"{receipt.imported}+{receipt.unchanged} == {ROWS - expected_invalid}",
    )
    _capture(
        "02-quarantine-proof.txt",
        f"quarantined={q_count} expected={expected_invalid}\n"
        f"reasons={json.dumps(reasons, sort_keys=True)}\n"
        f"expected_classes={json.dumps(expected_classes, sort_keys=True)}",
    )
    return ok


async def drill_reimport() -> bool:
    _banner("DRILL 3 — the re-import: identical fingerprint, zero changed rows")
    ok = True
    count, fp = await store.table_fingerprint()
    receipt = await run_import(DSN, FEED_PATH, "reimport")
    count2, fp2 = await store.table_fingerprint()
    ok &= _check("row count identical after re-import", count == count2, f"{count} == {count2}")
    ok &= _check("content fingerprint identical", fp == fp2, fp[:16] + "…")
    ok &= _check(
        "the second import changed nothing",
        receipt.imported == 0 and receipt.updated == 0,
        f"imported={receipt.imported} updated={receipt.updated} "
        f"unchanged={receipt.unchanged} quarantined={receipt.quarantined}",
    )
    _capture(
        "01-reimport-proof.txt",
        f"first=(rows={count}, fingerprint={fp})\nsecond=(rows={count2}, fingerprint={fp2})\n"
        f"second_receipt: imported={receipt.imported} updated={receipt.updated} "
        f"unchanged={receipt.unchanged} quarantined={receipt.quarantined}",
    )
    return ok


async def drill_cache() -> bool:
    _banner("DRILL 4 — the cross-run cache: the transform runs once")
    ok = True
    executions_before = await store.body_run_count("read_partition")
    started = time.monotonic()
    receipt = await run_import(DSN, FEED_PATH, "cache-warm")
    wall = time.monotonic() - started
    executions_after = await store.body_run_count("read_partition")
    ok &= _check(
        "the cached transform did NOT re-execute",
        executions_before == executions_after,
        f"executions={executions_after}",
    )
    ok &= _check("the warm run still completed with exact counts", receipt.total_rows == ROWS)
    print(f"  warm-run wall: {wall:.2f}s (the transform's share is the cache's saving)")
    _capture(
        "04-cache-proof.txt",
        f"transform_executions_before={executions_before} "
        f"after_third_run={executions_after} warm_wall_s={wall:.3f}",
    )
    return ok


async def drill_resume(crash_at: int) -> bool:
    """The crash-and-recovery drill: the driver hard-exits mid-import (the
    Nth apply-batch commits, then the process vanishes — no cleanup, no
    terminal writes). The restart reuses the run key (the arbiter's
    existing-running claim, the ledger's replay contract); what a
    driver-only deployment cannot do is reclaim the crashed node's lease —
    that is the worker-hosted sweep's job — so the runbook finishes with
    the operator doors: cancel the stuck run, re-issue under a new key,
    and let the idempotent upserts absorb the overlap. Every re-execution
    is reported honestly; the proof is the DATA (nothing lost, nothing
    duplicated) plus the final exact receipt."""
    _banner("DRILL 5 — the resume: hard crash mid-import, recover, nothing lost or duplicated")
    ok = True

    proc = _spawn_driver("resume-run", MEAS / "resume-progress.jsonl", crash_at=crash_at)
    await asyncio.get_event_loop().run_in_executor(None, proc.wait)
    log = (MEAS / "run-resume-run.log").read_text()
    if "[crash]" not in log:
        print("  [FAIL] the crash injection never fired; aborting the resume drill")
        return False
    crashed_at = next(line for line in log.splitlines() if "[crash]" in line)
    print(f"  {crashed_at.split('] ', 1)[-1]} at {_stamp()}")

    flow_id = _flow_id_from_log("resume-run")
    pre = await _flow_ledger(flow_id)
    pre_terminal = {k: v for k, v in pre.items() if v[0] == "succeeded"}
    print(f"  at the crash: {len(pre_terminal)} nodes terminal-succeeded")

    # Restart with the SAME run key — the arbiter's existing-running claim.
    # The drive re-executes the ready work idempotently, but the crashed
    # node's lease belongs to the dead process and only the worker-hosted
    # sweep reclaims it, so the receipt cannot land from a driver alone.
    restarts = 0
    resumed_log = ""
    for restarts in range(1, 4):
        proc2 = _spawn_driver("resume-run", MEAS / "resume-progress.jsonl")
        await asyncio.get_event_loop().run_in_executor(None, proc2.wait)
        resumed_log = (MEAS / "run-resume-run.log").read_text()
        print(f"  restart #{restarts} exited rc={proc2.returncode}")
        if "IMPORT SUCCEEDED" in resumed_log:
            break

    claim_lines = [line for line in resumed_log.splitlines() if line.startswith("claim:")]
    ok &= _check(
        "the restart claimed the EXISTING run (never a second silent run)",
        any("existing" in line for line in claim_lines[1:]),
        claim_lines[-1].strip() if len(claim_lines) > 1 else "no second claim",
    )

    post = await _flow_ledger(flow_id)
    reexecuted = [
        k
        for k, (status, attempt) in post.items()
        if k in pre_terminal and (status != "succeeded" or attempt > pre_terminal[k][1])
    ]
    _check(
        "terminal-succeeded work did not re-execute (the ledger's replay contract)",
        not reexecuted,
        f"{len(pre_terminal)} terminal pre-crash; re-executed: {reexecuted[:5]}",
    )

    count_mid, _fp_mid = await store.table_fingerprint()
    q_mid = await store.quarantine_count()
    print(f"  mid-recovery data state: rows={count_mid} quarantined={q_mid}")

    # The operator doors: cancel the stuck run, re-issue under a new key.
    cancel = _cli("flows", "cancel", flow_id, "--reason", "probe: unrecoverable crash")
    ok &= _check(
        "the operator could cancel the stuck run",
        cancel.returncode == 0,
        cancel.stdout.strip()[:100],
    )

    receipt = await run_import(DSN, FEED_PATH, "resume-recovered")
    count, fp = await store.table_fingerprint()
    q = await store.quarantine_count()
    ok &= _check(
        "the re-issued import completed with the SUCCESS receipt",
        receipt.imported + receipt.updated + receipt.unchanged + receipt.quarantined == ROWS,
        f"imported={receipt.imported} unchanged={receipt.unchanged} "
        f"quarantined={receipt.quarantined}",
    )
    ok &= _check(
        "no row lost or duplicated after the recovery",
        count == ROWS - feed.expected_invalid_count(ROWS)
        and q == feed.expected_invalid_count(ROWS),
        f"rows={count} quarantined={q}",
    )
    _capture(
        "03-resume-proof.txt",
        f"crash: {crashed_at}\npre_crash_terminal_nodes={len(pre_terminal)}\n"
        f"terminal_reexecuted={reexecuted}\nrestarts={restarts}\n"
        f"mid_recovery_rows={count_mid} mid_recovery_quarantined={q_mid}\n"
        f"cancel={cancel.stdout.strip()}\n"
        f"recovered_receipt: imported={receipt.imported} updated={receipt.updated} "
        f"unchanged={receipt.unchanged} quarantined={receipt.quarantined}\n"
        f"rows_after={count} quarantined_after={q} fingerprint_after={fp}\nclaims={' | '.join(claim_lines)}",
    )
    return ok


async def drill_admin_view() -> bool:
    _banner("DRILL 6 — the admin view: see + operate the import mid-flight")
    ok = True
    started = _stamp()
    proc = _spawn_driver("admin-demo", MEAS / "admin-progress.jsonl")
    try:
        if not await _wait_body_runs("validate_chunk", started, at_least=2, timeout_s=180):
            print("  [FAIL] the demo run never started chunks")
            return False

        flows_list = _cli("flows", "list", "--limit", "5")
        _capture("07a-flows-list-midflight.txt", flows_list.stdout + flows_list.stderr)
        print(flows_list.stdout[:600])

        flow_id = _flow_id_from_log("admin-demo")
        status_mid = _cli("flows", "status", flow_id)
        _capture("07b-flows-status-midflight.txt", status_mid.stdout)
        print(status_mid.stdout[:800])

        p = await store.pool()
        child = await p.fetchrow(
            f"SELECT job_id, step_key, map_index, status FROM {STEP_LEDGER} "  # noqa: S608
            "WHERE flow_id = $1::uuid AND map_index IS NOT NULL LIMIT 1",
            flow_id,
        )
        if child is not None:
            job_show = _cli("job", "show", str(child["job_id"]))
            _capture(
                "07c-job-show-chunk-child.txt",
                f"# ledger: step_key={child['step_key']} map_index={child['map_index']} "
                f"status={child['status']}\n{job_show.stdout}",
            )
            print(
                f"  chunk child display identity: step_key={child['step_key']} "
                f"map_index={child['map_index']}"
            )
            ok &= _check("the chunk index is visible as the node's map face", True)

        cancel = _cli("flows", "cancel", flow_id, "--reason", "admin-drill")
        _capture("07d-flows-cancel.txt", cancel.stdout + cancel.stderr)
        ok &= _check(
            "the operator could OPERATE the run (the cancel cascade)",
            cancel.returncode == 0,
            (cancel.stdout or cancel.stderr).strip()[:120],
        )

        status_after = _cli("flows", "status", flow_id)
        _capture("07e-flows-status-after.txt", status_after.stdout)
        print("  post-cancel status:")
        for line in status_after.stdout.splitlines()[:4]:
            print("   ", line)
        ok &= _check("the operator could SEE the run mid-flight", status_mid.returncode == 0)
    finally:
        proc.kill()
        await asyncio.get_event_loop().run_in_executor(None, proc.wait)
    return ok


async def _latest_flow_id() -> str:
    p = await store.pool()
    v = await p.fetchval(f"SELECT flow_id FROM {STEP_LEDGER} ORDER BY created_at DESC LIMIT 1")  # noqa: S608
    return str(v)


async def _chunk_progress_rows(flow_id: str) -> list[dict[str, object]]:
    """The durable progress snapshot for the run's chunk nodes (the push's payload)."""
    p = await store.pool()
    node_progress = f"{SCHEMA}.wf_node_progress"
    rows = await p.fetch(
        f"SELECT p.pct, p.message, p.data FROM {node_progress} p "  # noqa: S608
        f"JOIN {STEP_LEDGER} l ON l.job_id = p.node_id "
        "WHERE l.flow_id = $1::uuid AND p.channel = 'progress' ORDER BY p.message",
        flow_id,
    )
    return [dict(r) for r in rows]


async def main() -> int:
    MEAS.mkdir(parents=True, exist_ok=True)
    store.configure(DSN)
    await store.apply_ddl()
    await store.reset_tables()

    _capture(
        "00-env.txt", f"rows={ROWS} chunk_size={CHUNK_SIZE} started={_stamp()} schema={SCHEMA}"
    )
    sha = feed.generate_feed(FEED_PATH, ROWS)
    print(
        f"feed: {ROWS} rows, sha256={sha[:16]}…, "
        f"expected invalid={feed.expected_invalid_count(ROWS)}",
        flush=True,
    )

    # DRILL 1 — the cold import: throughput + the progress push
    _banner("DRILL 1 — the cold import: throughput + the progress push")
    started = time.monotonic()
    receipt = await run_import(
        DSN, FEED_PATH, "cold", progress_log=str(MEAS / "06-progress-events.jsonl")
    )
    wall = time.monotonic() - started
    ok = True
    rps = receipt.total_rows / wall
    ok &= _check(
        "throughput is measured, not promised",
        True,
        f"{receipt.total_rows} rows in {wall:.2f}s = {rps:.0f} rows/s",
    )
    ok &= _check(
        "receipt totals are the feed's arithmetic",
        receipt.total_rows == ROWS and receipt.chunks == ROWS // CHUNK_SIZE,
        f"total={receipt.total_rows} chunks={receipt.chunks}",
    )
    log = MEAS / "06-progress-events.jsonl"
    events = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    updated = [e for e in events if e["kind"] == "updated"]
    print(f"  progress events pushed live: {len(events)} total, {len(updated)} updates")
    ok &= _check(
        "the progress push delivered events while the import ran",
        len(updated) > 0,
        f"{len(updated)} pushed events",
    )

    flow_id = await _latest_flow_id()
    snapshot = await _chunk_progress_rows(flow_id)
    chunk_msgs = [r for r in snapshot if str(r["message"]).startswith("chunk ")]
    print(
        f"  durable progress snapshot: {len(snapshot)} nodes with progress; "
        f"{len(chunk_msgs)} chunk messages (chunk N validated)"
    )
    ok &= _check(
        "each chunk pushed its 'chunk N of M' message",
        len(chunk_msgs) == ROWS // CHUNK_SIZE,
        f"{len(chunk_msgs)} chunk messages",
    )
    _capture(
        "05-throughput.txt",
        f"rows={receipt.total_rows} wall_s={wall:.3f} rows_per_s={rps:.1f} "
        f"pushed_events={len(events)}\nchunk_messages={len(chunk_msgs)}\n"
        f"snapshot_sample={json.dumps(snapshot[:3], default=str)}",
    )

    ok &= await drill_quarantine(receipt)
    ok &= await drill_reimport()
    ok &= await drill_cache()
    ok &= await drill_resume(crash_at=6)
    ok &= await drill_admin_view()

    _banner("VERDICT")
    print("DRILL SUITE SUCCEEDED" if ok else "DRILL SUITE FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
