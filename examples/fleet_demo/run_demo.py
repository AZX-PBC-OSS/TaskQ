"""The fleet demo's orchestrator: one script, seven acts, real containers.

Run from the repo root:

    uv run python -m examples.fleet_demo.run_demo

The script brings up the demo's own Postgres 18 + Redis (compose), applies
migrations, runs a real ``taskq worker`` subprocess, and walks the fleet
story through it:

  1. enqueue → dispatch → succeed
  2. rate limiting: the token bucket's and the GCRA window's denials,
     each with its Retry-After, and the job rows they reschedule
  3. the operator cancel: the ownership verdict — cancelled, never abandoned
  4. the cron tick budget (the budget DEFERRAL) and the catch-up window
     (the budget SKIPPING, then the sequential catch-up crawl)
  5. the deploy story: SIGTERM mid-job — the job is interrupted and
     released, a deploy never terminalises a row
  6. ``taskq doctor`` on a deliberately sick config: the finding families
  7. ``taskq insights`` reading the same run back

Every act prints the worker's own structlog events as its proof. Nothing
here talks to anything but Postgres and Redis on localhost.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_DIR = Path(__file__).resolve().parent
COMPOSE = (
    "docker",
    "compose",
    "-p",
    "taskq-fleet-demo",
    "-f",
    str(DEMO_DIR / "docker-compose.yml"),
)
PG_DSN = "postgresql://taskq:taskq@localhost:5433/taskq"
REDIS_URL = "redis://localhost:6380/0"
BASE_ENV = {
    **os.environ,
    "TASKQ_PG_DSN": PG_DSN,
    "TASKQ_REDIS_URL": REDIS_URL,
    "TASKQ_ENVIRONMENT": "dev",
    "TASKQ_QUEUES": "fleet",
}
BANNER = f"\n{'═' * 72}\n"


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> str:
    """Run a command to completion, return stdout, die loudly on failure."""
    # S603: every invocation is a fixed literal list this script itself
    # constructs (compose/psql/taskq) — nothing here is operator input.
    proc = subprocess.run(  # noqa: S603
        cmd,
        cwd=REPO_ROOT,
        env=env or BASE_ENV,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout


def _banner(title: str) -> None:
    print(f"{BANNER}{title}\n{'─' * 72}")


def _worker_log_lines(log_path: Path, needle: str) -> list[str]:
    """Every worker-log line whose JSON `event` field matches *needle*."""
    hits: list[str] = []
    for line in log_path.read_text(errors="replace").splitlines():
        if f'"event":"{needle}"' in line:
            hits.append(line)
    return hits


def _print_log_events(log_path: Path, needle: str, *, limit: int = 4) -> int:
    import json

    hits = _worker_log_lines(log_path, needle)
    print(f"  worker events “{needle}”: {len(hits)}")
    for line in hits[-limit:]:
        row = json.loads(line)
        keep = {
            k: row[k]
            for k in (
                "event",
                "actor",
                "bucket_name",
                "backend",
                "allowed",
                "remaining",
                "retry_after_seconds",
                "skipped_slots",
                "skipped_slots_partial",
                "next_fire_at",
                "schedule_id",
            )
            if k in row
        }
        print(f"    {keep}")
    return len(hits)


async def _wait_for_status(handle: Any, want: str, *, budget_s: float = 30.0) -> str:
    """Poll a job handle until it reaches *want*; return the last status."""
    deadline = time.monotonic() + budget_s
    status = "?"
    while time.monotonic() < deadline:
        status = str(await handle.status())
        if status == want:
            return status
        await asyncio.sleep(0.25)
    return status


def _wait_for_log(log_path: Path, needle: str, *, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _worker_log_lines(log_path, needle):
            return True
        time.sleep(0.5)
    return False


# ── The acts ────────────────────────────────────────────────────────────


def act0_infra() -> None:
    _banner("ACT 0 — infrastructure: compose up + migrate up")
    print("  docker compose -p taskq-fleet-demo up -d --wait …")
    _run([*COMPOSE, "up", "-d", "--wait"])
    print("  taskq migrate up …")
    out = _run(["uv", "run", "taskq", "migrate", "up"])
    applied = [line.strip() for line in out.splitlines() if line.strip().endswith(".sql")]
    print(f"  migrations applied: {len(applied)}")


def act1_enqueue_dispatch(log_path: Path) -> None:
    _banner("ACT 1 — enqueue → dispatch → succeed")

    async def body() -> None:
        from examples.fleet_demo.actors import ShipPayload, ship_order
        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            handles = []
            for i in range(3):
                handles.append(
                    await tq.enqueue(ship_order, ShipPayload(order_id=f"ORD-{1000 + i}"))
                )
            print(
                f"  enqueued {len(handles)} ship_order jobs: {[str(h.job_id)[:8] for h in handles]}"
            )
            for h in handles:
                status = await _wait_for_status(h, "succeeded")
                print(f"    {str(h.job_id)[:8]} → {status}")

    asyncio.run(body())
    print("\n  the worker's own dispatch/state events for the last job:")
    for line in _worker_log_lines(log_path, "dispatch")[-1:]:
        print(f"    {line[:160]}…")
    for line in _worker_log_lines(log_path, "state-change")[-2:]:
        print(f"    {line[:160]}…")


def act2_rate_limiting(log_path: Path) -> None:
    _banner("ACT 2 — rate limiting: the denial + the Retry-After")

    async def body() -> None:
        from examples.fleet_demo.actors import MeterPayload, token_metered, window_metered
        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            token_jobs = [
                await tq.enqueue(token_metered, MeterPayload(batch="token")) for _ in range(6)
            ]
            window_jobs = [
                await tq.enqueue(window_metered, MeterPayload(batch="window")) for _ in range(6)
            ]
            print(
                "  enqueued 6 token_metered (bucket: capacity 2, refill 1/s) + 6 window_metered (GCRA: 3 per 15s)"
            )
            for h in (*token_jobs, *window_jobs):
                status = await _wait_for_status(h, "succeeded", budget_s=60.0)
                print(f"    {str(h.job_id)[:8]} → {status}")

    asyncio.run(body())

    print("\n  the registry's decision log: each denial carries its Retry-After")
    _print_log_events(log_path, "rate-limit-decision", limit=6)

    print("\n  what the denial did to the job rows: a denied dispatch re-pends the")
    print("  row as 'scheduled' with scheduled_at = now + retry_after (the Retry-")
    print("  After), never consuming retry budget — the insights wait split below")
    print("  reads these as the 'deferred' segment.")
    _run_cli(["taskq", "insights", "wait", "--window", "1h"])


def act3_operator_cancel(log_path: Path) -> None:
    _banner("ACT 3 — the operator cancel: the ownership verdict")

    async def body() -> str:
        from examples.fleet_demo.actors import LongHaulPayload, long_haul
        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            handle = await tq.enqueue(long_haul, LongHaulPayload(seconds=45))
            print(f"  enqueued long_haul {str(handle.job_id)[:8]} (45s of work)")
            status = await _wait_for_status(handle, "running")
            print(f"    row is {status}")
            return str(handle.job_id)

    job_id = asyncio.run(body())
    print("\n  the operator cancels it through the CLI:")
    out = _run_cli(["taskq", "job", "cancel", job_id, "--reason", "operator drill"])
    print("  " + out.strip().replace("\n", "\n  "))

    async def wait_terminal() -> None:
        from uuid import UUID

        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            handle = await tq.get(UUID(job_id))
            assert handle is not None
            status = await _wait_for_status(handle, "cancelled")
            print(f"\n  the row terminalised: {status}")
            print("    the verdict is 'cancelled' — an operator cancel owns the row;")
            print("    'abandoned' is never its outcome, the cancel bookkeeping")
            print("    (cancel_requested_at, cancel_phase) stays on the row. The")
            print("    operator-vs-shutdown ORIGIN is the worker's routing stamp —")
            print("    it picked who owns this write; act 5's cancel_origin_counts")
            print("    event is where a shutdown origin surfaces.")

    asyncio.run(wait_terminal())

    print("\n  the job's event trail (the cancel_request entry with the operator's reason):")
    out = _run_cli(["taskq", "job", "events", job_id])
    for line in out.splitlines():
        if line.strip():
            print(f"    {line.strip()[:170]}")


def act4_cron_budget(log_path: Path) -> None:
    _banner("ACT 4 — the cron tick budget + the catch-up window")
    print("  three cron schedules are live (every worker startup registers them):")
    print("    cron_digest      */15  — fast factory, the honest peer")
    print("    cron_greedy_one  */5   — factory burns 4.0s of the tick's 4.5s funded budget")
    print("    cron_greedy_two  */5   — same; the tick can fund ONE of the two")
    print()
    print("  watching the leader's cron loop for 12s …")
    time.sleep(12)
    print("\n  the tick-budget refusal: every tick that fires a greedy factory")
    print("  leaves a leftover (≈0.45s) below the minimum fundable grant (a")
    print("  quarter of the funded budget) — so the NEXT schedule in the tick's")
    print("  plan (the other greedy, and the honest digest peer on greedy ticks)")
    print("  is DEFERRED: next_fire_at advances one cadence, no strike, no retry")
    print("  budget burned:")
    _print_log_events(log_path, "cron-fire-budget-deferred", limit=4)

    print("\n  ── the catch-up window's BUDGET SKIPPING ──")
    print("  the operator mutes the monopolizers (and lets the in-flight tick's")
    print("  own next_fire_at write settle — an operator replay that raced the")
    print("  leader's suppression commit would be clobbered by it):")
    _run_psql("UPDATE taskq.cron_schedules SET enabled = false WHERE actor LIKE 'cron_greedy%'")
    time.sleep(6)
    print("  now replay cron_digest's clock 65 minutes into the past — OUTSIDE")
    print("  the 1h catch-up window. The tick admits it lost the race: it drops")
    print("  every owed slot and hops to the next future occurrence (this is the")
    print("  window acting as a budget: it bounds how far back the fleet will pay):")
    _run_psql(
        "UPDATE taskq.cron_schedules SET next_fire_at = now() - interval '65 minutes' "
        "WHERE actor = 'cron_digest'"
    )
    if not _wait_for_log(log_path, "cron missed slots skipped", timeout=45):
        raise SystemExit("expected the catch-up skip event; none arrived")
    _print_log_events(log_path, "cron missed slots skipped", limit=2)

    print("\n  ── the catch-up CRAWL ──")
    print("  now the replay lands INSIDE the window: 75 seconds back at a 15s")
    print("  cadence = 5 owed slots. Each tick fires the oldest owed slot and")
    print("  advances next_fire_at by one cadence — the sequential catch-up crawl,")
    print("  no budget consumed, no retry spent:")
    time.sleep(6)
    digest_fires_before = len(
        [
            line
            for line in _worker_log_lines(log_path, "cron fired")
            if '"actor":"cron_digest"' in line
        ]
    )
    _run_psql(
        "UPDATE taskq.cron_schedules SET next_fire_at = now() - interval '75 seconds' "
        "WHERE actor = 'cron_digest'"
    )
    time.sleep(9)
    fires = _worker_log_lines(log_path, "cron fired")
    crawl = [line for line in fires if '"actor":"cron_digest"' in line][digest_fires_before:]
    print(f"  digest 'cron fired' events during the crawl: {len(crawl)}")
    import json

    # The listing shows at most SIX lines; a longer crawl says so (the
    # stranger test's cosmetic finding: the count said 7, the listing
    # showed 6, nothing named the difference).
    for line in crawl[:6]:
        row = json.loads(line)
        print(
            f"    actor={row['actor']} fired_at={row['timestamp'][11:19]} skipped_slots={row['skipped_slots']}"
        )
    if len(crawl) > 6:
        print(f"    ... and {len(crawl) - 6} more")


def act5_sigterm_deploy(log_path: Path) -> None:
    _banner("ACT 5 — the deploy story: SIGTERM mid-job")
    print("  a deploy is an infrastructure event: it never terminalises a job.")

    async def body() -> str:
        from examples.fleet_demo.actors import LongHaulPayload, long_haul
        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            handle = await tq.enqueue(long_haul, LongHaulPayload(seconds=45))
            status = await _wait_for_status(handle, "running")
            print(f"  enqueued long_haul {str(handle.job_id)[:8]} — row is {status}")
            return str(handle.job_id)

    job_id = asyncio.run(body())
    worker_pid = int((log_path.parent / "worker.pid").read_text())

    print("\n  SIGTERM #1 (the deploy arrives) …")
    os.kill(worker_pid, signal.SIGTERM)
    time.sleep(1.5)
    print("  SIGTERM #2 (the orchestrator's escalation probe) …")
    try:
        os.kill(worker_pid, signal.SIGTERM)
    except ProcessLookupError:
        print("    (worker already exited)")
    code = _wait_process(worker_pid)
    print(f"  worker exited (code {code})")
    time.sleep(1)

    print("\n  the shutdown orchestration's phases, from the worker log:")
    import json

    for line in _worker_log_lines(log_path, "shutdown-phase"):
        row = json.loads(line)
        detail = {
            k: row[k] for k in ("phase", "active_jobs_count", "cancel_origin_counts") if k in row
        }
        print(f"    {detail}")

    async def final_row() -> None:
        from uuid import UUID

        from taskq import TaskQ

        async with TaskQ(dsn=PG_DSN, schema="taskq") as tq:
            handle = await tq.get(UUID(job_id))
            assert handle is not None
            row = await handle.refresh()
            print(f"\n  the interrupted job's row after the deploy: status={row.status}")
            print("    released back to the fleet with its spent attempt standing —")
            print("    NOT failed, NOT abandoned: nobody lied about what happened.")

    asyncio.run(final_row())


def act6_doctor() -> None:
    _banner("ACT 6 — taskq doctor on a deliberately sick config")
    print("  the worker is DOWN (act 5's deploy took it). Staging the sickness:")
    print("    1. a leftover operator tune: max_pending 1 below max_concurrent 4")
    out = _run_cli(
        [
            "taskq",
            "actor-config",
            "set",
            "ship_order",
            "--max-concurrent",
            "4",
            "--max-pending",
            "1",
        ]
    )
    print("       " + out.strip().splitlines()[-1][:150])
    print("    2. a stale queues row capping a queue no actor is assigned to")
    out = _run_cli(
        ["taskq", "queues", "set-max-concurrent", "legacy-ingest", "--max-concurrent", "4"]
    )
    print(f"       {out.strip()[:150]}")
    print("    3. a typo'd TASKQ_ env var (set for the doctor invocation)")
    print("    4. a platform stop grace of 1s (below the modelled worst case)")
    print("    5. offline_meter: in the doctor's registry only, its queue never")
    print("       subscribed — a second actor with no stored actor_config row")
    print("    6. ghost_job: in the doctor's registry, absent from the worker's,")
    print("       so it has no stored actor_config row")
    time.sleep(8)  # let the worker's liveness fully expire

    print("\n  the report (read-only, exit 0, safe mid-incident):")
    env = {**BASE_ENV, "TASKQ_NOT_A_REAL_KNOB": "1"}
    out = _run(
        [
            "uv",
            "run",
            "taskq",
            "doctor",
            "--actors",
            "examples.fleet_demo.actors:SICK_ACTORS",
            "--platform-grace-seconds",
            "1",
        ],
        env=env,
    )
    print("  " + out.strip().replace("\n", "\n  "))


def act7_insights() -> None:
    _banner("ACT 7 — taskq insights reads the same run back")
    out = _run_cli(["taskq", "insights"])
    print("  " + out.strip().replace("\n", "\n  "))
    print(
        "\n  wait: the 'deferred' segment IS the rate-limit act — rows whose\n"
        "  scheduled_at a Retry-After moved forward. balance: the worker is\n"
        "  down, so the fleet shows 'no capacity'. cron: the digest schedule's\n"
        "  ledger, fires vs cleared, the same run the acts above produced."
    )


def act_cleanup() -> None:
    _banner("CLEANUP — compose down -v (leave no volume behind)")
    print("  docker compose -p taskq-fleet-demo down -v …")
    _run([*COMPOSE, "down", "-v"])
    print("  demo containers and volume dropped — nothing left running.")


# ── plumbing ────────────────────────────────────────────────────────────


def _run_cli(cmd: list[str]) -> str:
    return _run(["uv", "run", *cmd])


def _run_psql(sql: str) -> None:
    _run(
        [
            "docker",
            "exec",
            _pg_container(),
            "psql",
            "-U",
            "taskq",
            "-d",
            "taskq",
            "-c",
            sql,
        ]
    )


def _pg_container() -> str:
    # S603: fixed literal argv (compose ps), nothing operator-supplied.
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "ps", "-q", "postgres"], cwd=REPO_ROOT, capture_output=True, text=True
    ).stdout.strip()
    if not out:
        raise SystemExit("postgres container not found — did act 0 run?")
    return out


def _wait_process(pid: int) -> int | None:
    import time as t

    while True:
        try:
            pid_, code = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return None
        if pid_ == pid:
            return os.waitstatus_to_exitcode(code)
        t.sleep(0.2)


def main() -> None:
    log_dir = Path.home() / ".cache" / "taskq-fleet-demo"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"worker-{int(time.time())}.log"

    act0_infra()
    _run_cli(["taskq", "migrate", "up"])  # idempotent

    _banner("starting the fleet worker (TASKQ_QUEUES=fleet)")
    log_file = open(log_path, "w")  # noqa: SIM115 — the worker subprocess owns it for the whole run
    # S607: fixed literal argv (uv + taskq), nothing operator-supplied.
    worker = subprocess.Popen(
        [  # noqa: S607
            "uv",
            "run",
            "taskq",
            "worker",
            "--actors",
            "examples.fleet_demo.actors:FLEET_ACTORS",
        ],
        cwd=REPO_ROOT,
        env=BASE_ENV,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    (log_dir / "worker.pid").write_text(str(worker.pid))
    print(f"  worker pid {worker.pid}, log {log_path}")
    try:
        if not _wait_for_log(log_path, "cron fired", timeout=60):
            print(worker_log_tail(log_path))
            raise SystemExit("worker never fired its first cron schedule")
        print("  worker is up, leader elected, cron loop live")

        act1_enqueue_dispatch(log_path)
        act2_rate_limiting(log_path)
        act3_operator_cancel(log_path)
        act4_cron_budget(log_path)
        act5_sigterm_deploy(log_path)
        act6_doctor()
        act7_insights()
        act_cleanup()
    finally:
        log_file.close()
        try:
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    worker.kill()
        except ProcessLookupError:
            pass  # act 5 already reaped the worker via os.waitpid


def worker_log_tail(log_path: Path) -> str:
    return "\n".join(log_path.read_text(errors="replace").splitlines()[-40:])


if __name__ == "__main__":
    main()
