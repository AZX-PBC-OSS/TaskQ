"""Executable walkthroughs of the documented operator flows.

Every test here walks a documented procedure EXACTLY as the docs write it -
the CLI steps run as real ``python -m taskq`` subprocesses against a real
Postgres container (the shared integration pair), the library steps run the
docs' own snippets. The docs these tests verify:

- ``docs/guides/ops.md`` §12 (scaling playbook: "a queue is backed up")
- ``docs/guides/cli.md`` (job show / cancel / retry / events / cancel-where,
  queues depth, actor-config, doctor, health, workgroup validate)
- ``docs/guides/deployment.md`` (bootstrap → health → verify loop)
- ``docs/guides/workgroups.md`` (``taskq workgroup validate``)
- ``docs/guides/observability.md`` + ``rules.yaml`` (the scrape → rule path)
- ``docs/guides/insights.md`` (the cron fan-out ledger and the imbalance /
  drain surfaces, flow: "diagnose a cron that's falling behind")

A step that drifts - a renamed command, a changed output shape, a finding
that stops appearing - fails here, so the docs' procedures cannot rot
silently. The doctor stale-worker test (``test_flow1_doctor_ignores_stale_
worker_row``) is the red proof for the defect it names: doctor's on-demand
stranded-jobs read must apply the same worker-liveness window the leader's
sweep applies, or a dead-but-unswept worker row hides an unserved queue from
the operator mid-incident.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.insights import (
    INSIGHTS_WINDOWS,
    fetch_cron_ledger,
    fetch_drain_estimates,
    fetch_queue_imbalance,
)

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[1]
#: The CLI invocation every docs page shows as ``taskq ...``; run through the
#: same interpreter the suite uses so no install step is assumed.
CLI = [sys.executable, "-m", "taskq"]
#: Host-importable actor registry (tests/e2e/host_actors.py) - the workers and
#: the ``--actors`` references in these walkthroughs load it exactly as a
#: deployment would.
ACTORS_REF = "tests.e2e.host_actors:ACTORS"
#: The queue that registry's actors declare (send_welcome_email/quick_result).
E2E_QUEUE = "e2e"

_WORKER_BOOT_TIMEOUT = 90.0


def _cli_env(schema_name: str, pg_dsn: str, **extra: str) -> dict[str, str]:
    env = dict(os.environ)
    env["TASKQ_PG_DSN"] = pg_dsn
    env["TASKQ_SCHEMA_NAME"] = schema_name
    env.update(extra)
    return env


def _run_cli(
    schema_name: str,
    pg_dsn: str,
    *args: str,
    timeout: float = 120.0,
    socket_path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one documented CLI step as a subprocess, exactly as an operator would."""
    extra: dict[str, str] = {}
    if socket_path is not None:
        extra["TASKQ_HEALTH_SOCKET_PATH"] = socket_path
    return subprocess.run(  # noqa: S603  # Why: static argv built from module constants, no shell.
        [*CLI, *args],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=_cli_env(schema_name, pg_dsn, **extra),
        timeout=timeout,
    )


async def _arun_cli(
    schema_name: str,
    pg_dsn: str,
    *args: str,
    timeout: float = 120.0,
    socket_path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """The same step, off the event loop (the walkthroughs are async tests)."""
    return await asyncio.to_thread(
        _run_cli, schema_name, pg_dsn, *args, timeout=timeout, socket_path=socket_path
    )


async def _insert_pending_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    n: int,
    payload: str = '{"key": "value"}',
) -> list[object]:
    """Seed pending job rows the way tests do (direct INSERT, due now)."""
    ids = []
    for _ in range(n):
        job_id = new_uuid()
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at
            ) VALUES ($1, $2, $3, $4::jsonb, 3, 'transient', 'pending', 0,
                      statement_timestamp())""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
            job_id,
            actor,
            queue,
            payload,
        )
        ids.append(job_id)
    return ids


def _queue_depth_row(stdout: str, queue: str) -> str | None:
    """The depth table's line for one queue, or None when absent."""
    for line in stdout.splitlines():
        if line.strip().split(" ", 1)[0] == queue or line.startswith(f"{queue} "):
            return line
    return None


async def _ensure_effects_table(conn: asyncpg.Connection, schema: str) -> None:
    """The e2e actors record their effects into this scratch table
    (tests/e2e/actors.py ``_record_effect``); a walkthrough schema must
    create it before a real worker dispatches."""
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}".e2e_effects ('
        "actor text, job_id uuid, attempt int, kind text, detail jsonb)"
    )


async def _bootstrap_worker_registers_actors(schema_name: str, pg_dsn: str) -> None:
    """The deployment docs' bootstrap step: a first worker startup seeds the
    actor_config rows. Run as ``--until-idle`` on an empty queue (exit 0)."""
    r = await _arun_cli(
        schema_name,
        pg_dsn,
        "worker",
        "--actors",
        ACTORS_REF,
        "--queues",
        E2E_QUEUE,
        "--until-idle",
        "--idle-settle-window",
        "1.0",
        "--idle-poll-interval",
        "0.5",
        timeout=120,
    )
    assert r.returncode == 0, f"bootstrap worker failed:\n{r.stdout}\n{r.stderr}"


# ── Flow 1: "a queue is backed up" (ops.md §12) ─────────────────────────


async def test_flow1_queue_backed_up_walkthrough(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """ops.md §12 + cli.md: observe depth → diagnose (doctor) → act (raise the
    cap, add a worker) → confirm it worked (depth drains, job succeeded)."""
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn

    # Deploy: the bootstrap startup seeds the actors' config rows.
    await _ensure_effects_table(clean_pg_conn, schema)
    await _bootstrap_worker_registers_actors(schema, dsn)

    # The backlog: five pending jobs, no worker serving the queue.
    ids = await _insert_pending_jobs(
        clean_pg_conn,
        schema,
        actor="send_welcome_email",
        queue=E2E_QUEUE,
        n=5,
        payload=json.dumps({"run_id": "walkthrough", "user_id": "u1", "email": "u1@example.com"}),
    )

    # Step 1 - observe depth (cli.md: `taskq queues depth`).
    r = await _arun_cli(schema, dsn, "queues", "depth")
    assert r.returncode == 0, r.stderr
    row = _queue_depth_row(r.stdout, E2E_QUEUE)
    assert row is not None, f"queues depth shows no {E2E_QUEUE} row:\n{r.stdout}"
    assert row.split()[1:3] == ["5", "0"], f"expected pending=5 scheduled=0, saw: {row}"

    # Step 2 - diagnose (ops.md §12: doctor is the mid-incident surface).
    # Remove the exited bootstrap worker's row so nothing counts as serving
    # the queue (the documented SQL in ops.md §8 is how an operator sees this).
    await clean_pg_conn.execute(f'DELETE FROM "{schema}".workers')  # noqa: S608  # Why: schema is identifier-validated upstream.
    r = await _arun_cli(schema, dsn, "doctor", "--actors", ACTORS_REF)
    assert r.returncode == 0, r.stderr  # doctor always exits 0 (documented)
    assert "stored actor capacity:" in r.stdout
    assert "send_welcome_email" in r.stdout
    assert f"'{E2E_QUEUE}'" in r.stdout and "no live worker serves" in r.stdout, (
        f"doctor did not name the unserved queue mid-backlog:\n{r.stdout}"
    )

    # Step 3 - act: the playbook's per-actor knob (live, no restart).
    r = await _arun_cli(
        schema, dsn, "actor-config", "set", "send_welcome_email", "--max-concurrent", "2"
    )
    assert r.returncode == 0, f"actor-config set failed:\n{r.stdout}\n{r.stderr}"
    r = await _arun_cli(schema, dsn, "actor-config", "get", "send_welcome_email")
    assert r.returncode == 0, r.stderr
    assert "max_concurrent=2" in r.stdout, f"the set did not read back:\n{r.stdout}"

    # Step 4 - act: add a worker (the playbook's replica knob) and confirm
    # it worked: the backlog drains, the jobs are terminal-success.
    r = await _arun_cli(
        schema,
        dsn,
        "worker",
        "--actors",
        ACTORS_REF,
        "--queues",
        E2E_QUEUE,
        "--until-idle",
        "--idle-settle-window",
        "1.0",
        "--idle-poll-interval",
        "0.5",
        timeout=180,
    )
    assert r.returncode == 0, f"draining worker failed:\n{r.stdout}\n{r.stderr}"

    r = await _arun_cli(schema, dsn, "job", "show", str(ids[0]))
    assert r.returncode == 0, r.stderr
    assert "status: succeeded" in r.stdout, f"job did not drain:\n{r.stdout}"

    r = await _arun_cli(schema, dsn, "queues", "depth")
    assert r.returncode == 0
    row = _queue_depth_row(r.stdout, E2E_QUEUE)
    drained = row is None or row.split()[1:2] == ["0"]
    assert drained, f"depth did not return to zero after the drain:\n{r.stdout}"


async def test_flow1_doctor_ignores_stale_worker_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """RED PROOF (src defect): doctor's stranded-jobs read must apply the
    worker-liveness window the leader sweep applies.

    cli.md documents doctor's unserved arm as "pending/scheduled jobs routed
    to a queue no LIVE worker serves" and says it is "the same computation
    the leader's stranded-jobs sweep runs every minute, issued here on
    demand". The leader's sweep (``_leader_sweeps.py``) filters worker rows
    by ``last_seen_at`` inside the liveness window - its own comment says a
    dead-but-unswept worker row "must not count as serving the queue".
    Doctor's copy did not carry that filter, so a stale worker row hid an
    unserved queue from the operator mid-incident: the exact false-green the
    sweep's comment exists to prevent.
    """
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn

    # A pending job for an actor whose config row exists (seeded), on the
    # queue a GHOST worker row still claims to subscribe.
    job_ids = await _insert_pending_jobs(
        clean_pg_conn, schema, actor="test_actor", queue="default", n=1
    )
    ghost = new_uuid()
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at)
            VALUES ($1, 'ghost-host', 1, $2,
                    statement_timestamp() - interval '10 minutes')""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        ghost,
        ["default"],
    )

    r = await _arun_cli(schema, dsn, "doctor", "--actors", ACTORS_REF)
    assert r.returncode == 0, r.stderr
    # The ghost's heartbeat is 10 minutes old (liveness window: 30 s), so the
    # queue must read as unserved and the finding must name it.
    assert "no live worker serves" in r.stdout, (
        "doctor let a stale worker row (last_seen 10 min old) count as serving "
        "the queue - the unserved-queue finding is hidden from the operator:\n"
        f"{r.stdout}"
    )
    assert str(job_ids[0]) not in r.stdout  # the finding names the actor, not the job

    # Control: the same fleet with a FRESH heartbeat for the worker row does
    # count as serving - no unserved finding for that queue.
    await clean_pg_conn.execute(f'DELETE FROM "{schema}".workers')  # noqa: S608  # Why: schema is identifier-validated upstream.
    fresh = new_uuid()
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at)
            VALUES ($1, 'live-host', 1, $2, statement_timestamp())""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        fresh,
        ["default"],
    )
    r = await _arun_cli(schema, dsn, "doctor", "--actors", ACTORS_REF)
    assert r.returncode == 0, r.stderr
    assert "no live worker serves" not in r.stdout, (
        f"a live worker subscribing the queue was read as unserved:\n{r.stdout}"
    )


async def test_flow1_act_branch_the_cap_knob_opens_and_closes_the_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """ops.md §12 row 1's remedy lever, driven CAUSALLY, both directions.

    The playbook's "first knob" for an actor-shaped backlog is
    ``taskq actor-config set ACTOR --max-concurrent N`` (live: "the
    dispatch query re-reads max_concurrent every dispatch cycle"). A
    walkthrough that only raises the cap next to a drain would pass even
    if the knob moved nothing - the worker drains regardless. This closes
    the loop in BOTH directions against a real worker:

    - the lever OFF (the documented ``0, DRAIN MODE`` value - cli.md
      documents the 0 state as "deliberately stopped; jobs enqueue and
      never run"): a real ``--until-idle`` worker cannot move the queue
      and exits 4 (the documented "idle-max-runtime exceeded before drain
      completed" code), jobs still pending;
    - the lever ON: the SAME command with N=2 unstrands the queue with no
      restart - the worker drains, jobs succeed, exit 0.

    Only the stored cap changed between the two legs; whatever moved the
    queue moved because the knob moved.
    """
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn

    await _ensure_effects_table(clean_pg_conn, schema)
    await _bootstrap_worker_registers_actors(schema, dsn)
    ids = await _insert_pending_jobs(
        clean_pg_conn,
        schema,
        actor="send_welcome_email",
        queue=E2E_QUEUE,
        n=2,
        payload=json.dumps({"run_id": "act-branch", "user_id": "u1", "email": "u1@example.com"}),
    )

    # The lever OFF: the documented DRAIN MODE value, read back as 0.
    r = await _arun_cli(
        schema, dsn, "actor-config", "set", "send_welcome_email", "--max-concurrent", "0"
    )
    assert r.returncode == 0, f"actor-config set 0 failed:\n{r.stdout}\n{r.stderr}"
    r = await _arun_cli(schema, dsn, "actor-config", "get", "send_welcome_email")
    assert "max_concurrent=0" in r.stdout, f"the 0 cap did not read back:\n{r.stdout}"

    # A real worker cannot drain a cap-0 queue: bounded --until-idle exits
    # 4, the jobs are still pending afterwards.
    r = await _arun_cli(
        schema,
        dsn,
        "worker",
        "--actors",
        ACTORS_REF,
        "--queues",
        E2E_QUEUE,
        "--until-idle",
        "--idle-settle-window",
        "1.0",
        "--idle-poll-interval",
        "0.5",
        "--idle-max-runtime",
        "8",
        timeout=120,
    )
    assert r.returncode == 4, (
        f"a cap-0 worker must exit 4 (idle-max-runtime exceeded), got "
        f"{r.returncode}:\n{r.stdout}\n{r.stderr}"
    )
    r = await _arun_cli(schema, dsn, "job", "show", str(ids[0]))
    assert "status: pending" in r.stdout, (
        f"a cap-0 queue drained anyway - DRAIN MODE does not stop dispatch:\n{r.stdout}"
    )

    # The lever ON: the same live command with N=2, no restart step.
    r = await _arun_cli(
        schema, dsn, "actor-config", "set", "send_welcome_email", "--max-concurrent", "2"
    )
    assert r.returncode == 0, f"actor-config set 2 failed:\n{r.stdout}\n{r.stderr}"

    r = await _arun_cli(
        schema,
        dsn,
        "worker",
        "--actors",
        ACTORS_REF,
        "--queues",
        E2E_QUEUE,
        "--until-idle",
        "--idle-settle-window",
        "1.0",
        "--idle-poll-interval",
        "0.5",
        timeout=180,
    )
    assert r.returncode == 0, f"the opened queue did not drain:\n{r.stdout}\n{r.stderr}"
    for job_id in ids:
        r = await _arun_cli(schema, dsn, "job", "show", str(job_id))
        assert "status: succeeded" in r.stdout, (
            f"job {job_id} did not run after the cap opened:\n{r.stdout}"
        )


# ── Flow 2: "a job is stuck" (cli.md job surfaces) ───────────────────────


async def test_flow2_stuck_job_walkthrough(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """cli.md: find it (job show) → diagnose (job events timeline) → act
    (cancel, retry) → verify; then the bulk path (cancel-where dry-run →
    apply) and the documented refusals."""
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn
    ids = await _insert_pending_jobs(
        clean_pg_conn, schema, actor="test_actor", queue="default", n=3
    )
    job_id = str(ids[0])

    # Find it.
    r = await _arun_cli(schema, dsn, "job", "show", job_id)
    assert r.returncode == 0, r.stderr
    assert "actor: test_actor" in r.stdout
    assert "queue: default" in r.stdout
    assert "status: pending" in r.stdout

    # Diagnose: the events timeline. A pending job has no transitions yet -
    # the documented empty answer, not an error.
    r = await _arun_cli(schema, dsn, "job", "events", job_id)
    assert r.returncode == 0, r.stderr
    assert "no job_events rows" in r.stdout

    # Act: cancel with a reason.
    r = await _arun_cli(schema, dsn, "job", "cancel", job_id, "--reason", "ops walkthrough")
    assert r.returncode == 0, f"cancel failed:\n{r.stdout}\n{r.stderr}"
    r = await _arun_cli(schema, dsn, "job", "show", job_id)
    assert r.returncode == 0
    assert "status: cancelled" in r.stdout, f"cancel did not land:\n{r.stdout}"

    # Verify: the timeline now carries the cancel request and the reason.
    r = await _arun_cli(schema, dsn, "job", "events", job_id)
    assert r.returncode == 0, r.stderr
    assert "cancel_request" in r.stdout, f"no cancel_request event:\n{r.stdout}"
    assert "ops walkthrough" in r.stdout, f"the reason is not on the timeline:\n{r.stdout}"

    # Act: retry the resting job.
    r = await _arun_cli(schema, dsn, "job", "retry", job_id)
    assert r.returncode == 0, f"retry failed:\n{r.stdout}\n{r.stderr}"
    assert "pending" in r.stdout  # documented: prints the read-back status
    r = await _arun_cli(schema, dsn, "job", "show", job_id)
    assert r.returncode == 0
    assert "status: pending" in r.stdout

    # Bulk: preview then apply, the same predicate set (cli.md's example).
    r = await _arun_cli(
        schema,
        dsn,
        "job",
        "cancel-where",
        "--queue",
        "default",
        "--status",
        "pending",
        "--dry-run",
    )
    assert r.returncode == 0, r.stderr
    assert "3" in r.stdout, f"dry-run did not report the matching count:\n{r.stdout}"
    r = await _arun_cli(
        schema,
        dsn,
        "job",
        "cancel-where",
        "--queue",
        "default",
        "--status",
        "pending",
        "--reason",
        "bulk walkthrough",
    )
    assert r.returncode == 0, f"bulk cancel failed:\n{r.stdout}\n{r.stderr}"
    for jid in ids:
        r = await _arun_cli(schema, dsn, "job", "show", str(jid))
        assert r.returncode == 0
        assert "status: cancelled" in r.stdout, f"job {jid} not bulk-cancelled:\n{r.stdout}"

    # Documented refusals: the guardrail and the unknown id.
    r = await _arun_cli(schema, dsn, "job", "cancel-where")
    assert r.returncode == 1, "the empty-filter guardrail did not refuse"
    unknown = str(new_uuid())
    r = await _arun_cli(schema, dsn, "job", "show", unknown)
    assert r.returncode == 1, "an unknown job id must exit 1"


# ── Flow 3: "deploy a new worker fleet" (deployment.md + workgroups.md) ──


async def _spawn_worker(
    schema: str,
    dsn: str,
    tmp_path: Path,
    *extra_args: str,
) -> tuple[subprocess.Popen[str], Path]:
    """Boot a real worker subprocess (deployment.md's container shape, on the
    host), logging to a file: a PIPE would fill while the worker runs and
    block it mid-boot."""
    sock = str(tmp_path / "walkthrough.sock")
    log_path = tmp_path / "worker.log"
    env = _cli_env(schema, dsn, TASKQ_HEALTH_SOCKET_PATH=sock)
    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(  # noqa: S603  # Why: static argv built from module constants, no shell.
        [
            *CLI,
            "worker",
            "--actors",
            ACTORS_REF,
            "--queues",
            E2E_QUEUE,
            "--health-socket-path",
            sock,
            *extra_args,
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc, log_path


async def _terminate_worker(proc: subprocess.Popen[str]) -> int:
    """SIGTERM and wait off-loop; the documented clean-shutdown contract."""
    proc.send_signal(signal.SIGTERM)
    try:
        return await asyncio.to_thread(proc.wait, 60)
    except subprocess.TimeoutExpired:
        proc.kill()
        await asyncio.to_thread(proc.wait, 10)
        raise


async def _await_liveness(
    schema: str, dsn: str, sock: str, proc: subprocess.Popen[str], log_path: Path
) -> None:
    """Poll `taskq health live` until the worker serves liveness."""
    deadline = datetime.now(UTC) + timedelta(seconds=_WORKER_BOOT_TIMEOUT)
    while True:
        r = await _arun_cli(schema, dsn, "health", "live", socket_path=sock, timeout=30)
        if r.returncode == 0:
            return
        if proc.poll() is not None:
            pytest.fail(
                f"worker exited before serving liveness:\n{log_path.read_text(encoding='utf-8')}"
            )
        assert datetime.now(UTC) < deadline, "worker never served /live"
        await asyncio.sleep(0.5)


async def test_flow3_worker_fleet_bootstrap_health_verify(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
    tmp_path: Path,
) -> None:
    """deployment.md/workgroups.md: migrate status → workgroup validate →
    worker boot → health live/ready/metrics → the workers-table verification
    row → clean SIGTERM shutdown (exit 0)."""
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn

    # Verify migrations (the deploy's first check).
    r = await _arun_cli(schema, dsn, "migrate", "status")
    assert r.returncode == 0, r.stderr
    assert f"schema: {schema}" in r.stdout, f"migrate status output drifted:\n{r.stdout}"

    # The workgroup config validates (workgroups.md's pre-deploy check).
    toml = tmp_path / "walkgroup.toml"
    toml.write_text(
        f'actors = "{ACTORS_REF}"\n'
        "\n"
        "[[workers]]\n"
        'name = "walkthrough-api"\n'
        f'queues = ["{E2E_QUEUE}"]\n'
        "max_concurrency = 2\n"
        "\n"
        "[[workers]]\n"
        'name = "walkthrough-batch"\n'
        f'queues = ["{E2E_QUEUE}"]\n'
        "poll_interval = 5.0\n"
        "max_concurrency = 1\n",
        encoding="utf-8",
    )
    r = await _arun_cli(schema, dsn, "workgroup", "validate", str(toml))
    assert r.returncode == 0, f"workgroup validate failed:\n{r.stdout}\n{r.stderr}"
    assert "config OK, 2 worker(s)" in r.stdout, f"validate output drifted:\n{r.stdout}"

    # Boot a real worker and walk the health surfaces.
    sock = str(tmp_path / "walkthrough.sock")
    proc, log_path = await _spawn_worker(
        schema, dsn, tmp_path, "--worker-label", "walkthrough-fleet"
    )
    try:
        await _await_liveness(schema, dsn, sock, proc, log_path)

        # Liveness and readiness (the exec-probe commands from deployment.md).
        r = await _arun_cli(schema, dsn, "health", "ready", socket_path=sock, timeout=30)
        assert r.returncode == 0, f"/ready not ready:\n{r.stdout}\n{r.stderr}"
        body = json.loads(r.stdout.strip().splitlines()[-1])
        assert body["ready"] is True
        assert "is_leader" in body and "shutdown_phase" in body

        # The health-socket metrics surface (cli.md `taskq health metrics`):
        # the three hand-rendered process gauges, leader state included.
        r = await _arun_cli(schema, dsn, "health", "metrics", socket_path=sock, timeout=30)
        assert r.returncode == 0, r.stderr
        for gauge in ("taskq_active_jobs", "taskq_is_leader", "taskq_shutdown_phase"):
            assert gauge in r.stdout, f"health metrics lost {gauge}:\n{r.stdout}"

        # The workers-table verification (deployment.md's own SQL shape):
        # the registered fleet row exists with our label and fresh heartbeat.
        row = await clean_pg_conn.fetchrow(
            f"""SELECT id FROM "{schema}".workers
                WHERE worker_label = 'walkthrough-fleet'
                AND last_seen_at > statement_timestamp() - interval '30 seconds'""",  # noqa: S608  # Why: schema is identifier-validated upstream.
        )
        assert row is not None, "the booted worker never registered a live workers row"
    finally:
        code = await _terminate_worker(proc)
    assert code == 0, (
        f"clean SIGTERM shutdown must exit 0 (docs), got {code}:\n"
        f"{log_path.read_text(encoding='utf-8')}"
    )


# ── Flow 4: "set up alerting" (observability.md + rules.yaml) ────────────
#
# The rules read WORKER-side series (ops.md §8: "the rules read worker-side
# series the admin process's /jobs/health/metrics never carries"), so the
# documented path is: set TASKQ_METRICS_PORT on every worker, scrape /metrics,
# import rules.yaml. This walkthrough boots a real worker with the port set
# and asserts the scrape serves the series the shipped rules' exprs read.


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_flow4_metrics_scrape_serves_the_series_the_rules_read(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
    tmp_path: Path,
) -> None:
    """observability.md: TASKQ_METRICS_PORT turns a real worker into a scrape
    target; the shipped rules' exprs read series that must appear on it.

    The sampler-fed gauges report rows, not zeroes (an empty population is
    an absent series, the docs' "absent, not 0" convention), so the walk
    seeds pending work first - including a job for an actor with no
    actor_config row, the stranded{reason="no_actor_config"} shape.
    taskq_heartbeat_misses_total is deliberately NOT asserted: it only
    exists after a failed heartbeat tick, and a healthy worker never
    records one.
    """
    schema, dsn = module_pg_schema.schema_name, module_pg_schema.pg_dsn
    port = _free_port()
    log_path = tmp_path / "worker-scrape.log"
    # The scrape listener binds the health host at TASKQ_METRICS_PORT.
    env = _cli_env(schema, dsn, TASKQ_METRICS_PORT=str(port))
    # Give the leader samplers a population to report.
    await _insert_pending_jobs(clean_pg_conn, schema, actor="test_actor", queue="default", n=2)
    await _insert_pending_jobs(
        clean_pg_conn, schema, actor="configless_actor", queue="default", n=1
    )
    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(  # noqa: S603  # Why: static argv built from module constants, no shell.
        [*CLI, "worker", "--actors", ACTORS_REF, "--queues", E2E_QUEUE],
        cwd=REPO_ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        url = f"http://127.0.0.1:{port}/metrics"
        deadline = datetime.now(UTC) + timedelta(seconds=_WORKER_BOOT_TIMEOUT + 60)
        required = (
            "taskq_worker_active_jobs",
            "taskq_worker_max_concurrency",
            "taskq_jobs_by_status",
            "taskq_jobs_oldest_pending_age_seconds",
            "taskq_queue_depth",
            "taskq_queue_live_workers",
            "taskq_maintenance_leader_is_leader",
            "taskq_maintenance_leader_sweep_last_success_seconds",
            "taskq_jobs_stranded",
            "taskq_jobs_scheduled_count",
            "taskq_jobs_oldest_due_age_seconds",
            "taskq_jobs_running_lease_expired",
        )
        body = ""
        while True:
            try:
                with urlopen(url, timeout=5) as resp:
                    body = (await asyncio.to_thread(resp.read)).decode("utf-8")
                missing = [s for s in required if s not in body]
                if not missing:
                    break
            except OSError:
                missing = list(required)  # listener not up yet
            if proc.poll() is not None:
                pytest.fail(
                    "worker exited before serving the scrape:\n"
                    f"{log_path.read_text(encoding='utf-8')}"
                )
            assert datetime.now(UTC) < deadline, (
                f"the scrape never served the sampler-fed series: {missing}"
            )
            await asyncio.sleep(2)
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            await asyncio.to_thread(proc.wait, 60)
        except subprocess.TimeoutExpired:
            proc.kill()
            await asyncio.to_thread(proc.wait, 10)
    log_file.close()

    log = log_path.read_text(encoding="utf-8")
    # The documented startup line, in the worker's default JSON shape: the
    # otel-exporter-configured event carrying the wired exporter and port
    # (the console renderer spells the same fields key=value; observability.md
    # documents both shapes).
    assert '"metrics":"prometheus"' in log and f'"prometheus_port":{port}' in log, (
        f"the scrape wiring startup line drifted:\n{log[-2000:]}"
    )

    # Every series above is present (the poll loop asserted the full set);
    # this is the docs' scrape → rule contract: the shipped rules' exprs
    # only ever name series a real worker scrape serves.


# ── Flow 5: "a cron that's falling behind" (insights.md) ─────────────────


async def test_flow5_cron_behind_insights_surfaces(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """insights.md: the documented snippets run as written against a real PG -
    the imbalance row's starvation shape, the cron ledger's keys, the drain
    estimate's confidence caveat."""
    schema = module_pg_schema.schema_name

    # The named windows the docs list.
    assert set(INSIGHTS_WINDOWS) == {"1h", "6h", "24h", "7d"}, (
        f"INSIGHTS_WINDOWS drifted from the documented set: {sorted(INSIGHTS_WINDOWS)}"
    )

    # Two pending jobs, no workers: the documented starvation shape
    # (`utilization IS NULL with depth > 0`).
    await _insert_pending_jobs(clean_pg_conn, schema, actor="test_actor", queue="default", n=2)
    rows = await fetch_queue_imbalance(clean_pg_conn, schema=schema, worker_liveness_seconds=30)
    by_queue = {r["queue"]: r for r in rows}
    assert "default" in by_queue, f"imbalance missed the queue: {rows}"
    row = by_queue["default"]
    # The documented column set.
    for col in (
        "depth",
        "scheduled_depth",
        "live_workers",
        "actor_capacity",
        "effective_capacity",
        "utilization",
        "oldest_due_age_s",
    ):
        assert col in row, f"imbalance row lost documented column {col}: {row.keys()}"
    assert row["depth"] == 2
    assert row["live_workers"] == 0
    assert row["utilization"] is None, (
        "due work with no live workers must read as the starvation shape"
    )

    # The cron ledger: the ledger reads one row per cron_schedules row, so
    # register a schedule, then seed one of its fires enqueued and cleared
    # inside the window (the metadata `cron_schedule_id` stamp is the
    # provenance the doc describes).
    schedule_id = new_uuid()
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".cron_schedules
                (id, actor, cron_expr, next_fire_at)
            VALUES ($1, 'test_actor', '* * * * *',
                    statement_timestamp() + interval '1 hour')""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        schedule_id,
    )
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                priority, scheduled_at, started_at, finished_at, metadata
            ) VALUES ($1, 'test_actor', 'default', '{{"key": "v"}}'::jsonb, 3,
                      'transient', 'succeeded', 0, statement_timestamp(),
                      statement_timestamp(), statement_timestamp(),
                      $2::jsonb)""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        new_uuid(),
        json.dumps({"cron_schedule_id": str(schedule_id)}),
    )
    ledger = await fetch_cron_ledger(clean_pg_conn, schema=schema, window=timedelta(hours=1))
    entry = next((r for r in ledger if str(r.get("schedule_id")) == str(schedule_id)), None)
    assert entry is not None, f"the ledger missed the stamped fire: {ledger}"
    for col in (
        "fires_window",
        "cleared_window",
        "fires_prior",
        "cleared_prior",
        "outstanding",
        "runaway_trending",
    ):
        assert col in entry, f"cron ledger row lost documented column {col}: {entry.keys()}"
    assert entry["fires_window"] >= 1
    assert entry["cleared_window"] >= 1
    assert isinstance(entry["runaway_trending"], bool)

    # The drain estimate: with traffic in the window, eta_seconds is a number
    # (the documented caveat - never a false zero when has_traffic is false).
    est = await fetch_drain_estimates(clean_pg_conn, schema=schema, window=timedelta(hours=1))
    drow = next((r for r in est if r["queue"] == "default"), None)
    assert drow is not None, f"drain estimate missed the queue: {est}"
    assert "eta_seconds" in drow and "has_traffic" in drow
    assert drow["has_traffic"] is True
    assert drow["eta_seconds"] is not None and drow["eta_seconds"] > 0
