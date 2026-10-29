"""The drills' shared harness: worker fleets, batch builders, receipts.

The drills are an ADOPTED PRODUCT's drills: real worker subprocesses, a
real Postgres, the adverse world injected at the mock boundary — and
every invariant asserted UNCONDITIONALLY. Nothing here weakens an
assertion to go green; a red is a finding, reported.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import asyncpg

REPO_ROOT = Path(__file__).resolve().parents[3]
MEASUREMENTS = REPO_ROOT / ".measurements" / "ledgerwatch"

#: The drill estate's PG (:5802 — this adoption's own instance).
TEST_DSN = os.environ.get(
    "LEDGERWATCH_TEST_DSN", "postgresql://postgres:taskq@localhost:5802/taskq"
)

#: The run's schema (one per pytest invocation; dropped at teardown).
RUN_STAMP = time.strftime("%Y%m%d-%H%M%S", time.gmtime()) + f"-{os.getpid() % 10000}"
SCHEMA = f"lw_{RUN_STAMP.replace('-', '_')}"

#: The env MUST be stamped before examples.ledgerwatch.app imports (the
#: app's deps binding reads the env at import — the worker fleet and
#: every trigger then agree on the estate).
os.environ["TASKQ_PG_DSN"] = TEST_DSN
os.environ["TASKQ_SCHEMA_NAME"] = SCHEMA
os.environ.setdefault("TASKQ_ENVIRONMENT", "dev")

#: The drill fleet's tuned runtime (tight crash-recovery latencies; the
#: values' invariants are the worker's own startup validation's).
FLEET_TUNING: dict[str, str] = {
    "TASKQ_QUEUES": "lw-cpu,lw-enrich,lw-recon",
    "TASKQ_MAX_CONCURRENCY": "32",
    "TASKQ_SWEEP_INTERVAL": "1",
    "TASKQ_HEARTBEAT_INTERVAL": "0.5",
    "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
    "TASKQ_MAX_HEARTBEAT_FAILURES": "1",
    "TASKQ_LOCK_LEASE": "3",
    "TASKQ_LEADER_LEASE": "4",
    # The watchdog, tuned ABOVE its own sampling cadence: a warn budget
    # below the check interval warns on every healthy beat, and each
    # warn's task dump blocks the loop — a self-sustaining storm (the
    # drill's finding F6; the values keep the invariants: warn < budget,
    # budget > check, budget + heartbeat < lock lease).
    "TASKQ_WATCHDOG_CHECK_INTERVAL": "1.0",
    "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "1.5",
    "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "2.0",
    "TASKQ_CANCELLATION_GRACE_PERIOD": "1",
    "TASKQ_CLEANUP_GRACE_PERIOD": "1",
    "TASKQ_ENVIRONMENT": "dev",
    "LW_PROFILE": "clean",
}


def worker_env(**overrides: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(FLEET_TUNING)
    env.update(overrides)
    env["DOTENV_DIR"] = str(MEASUREMENTS / "empty-env-dir")
    os.makedirs(env["DOTENV_DIR"], exist_ok=True)
    return env


class Worker:
    """One REAL ``taskq worker`` subprocess (the fleet's member)."""

    def __init__(self, name: str, **env_overrides: str) -> None:
        self.name = name
        log_path = MEASUREMENTS / "logs" / f"worker-{name}-{RUN_STAMP}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path = log_path
        log = open(log_path, "w")  # noqa: SIM115  # Why: the worker's log lives for the process's whole life — the handle is the fleet's, closed with the process.
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "examples.ledgerwatch.worker"],
            cwd=REPO_ROOT,
            env=worker_env(**env_overrides),
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    @property
    def pid(self) -> int:
        return self._proc.pid

    def terminate(self) -> None:
        """SIGTERM (the graceful stop's request)."""
        self._proc.terminate()

    def wait(self, timeout: float | None = None) -> int | None:
        return self._proc.wait(timeout=timeout)

    def sigkill(self) -> None:
        """THE CRASH: SIGKILL — no drain, no cleanup, no goodbye."""
        self._proc.send_signal(signal.SIGKILL)
        self._proc.wait()
        self.alive = False

    alive = True

    def __repr__(self) -> str:
        return f"<Worker {self.name} pid={self._proc.pid} alive={self.alive}>"


class Fleet:
    """The worker fleet: spawn, readiness, kill, replace."""

    def __init__(self, schema: str) -> None:
        self.schema = schema
        self.workers: list[Worker] = []

    async def start(self, count: int = 1, **env_overrides: str) -> None:
        for i in range(count):
            self.workers.append(Worker(f"{len(self.workers)}-{i}", **env_overrides))
        await self.wait_ready(min_alive=count)

    async def wait_ready(self, min_alive: int = 1, timeout_s: float = 60.0) -> None:
        """The fleet is ready when its workers heartbeat the DB."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            conn = await asyncpg.connect(TEST_DSN)
            try:
                alive = await conn.fetchval(
                    f'SELECT count(*) FROM "{self.schema}".workers'  # noqa: S608  # Why: the schema identifier only.
                    " WHERE last_seen_at > now() - interval '5 seconds'",
                )
            finally:
                await conn.close()
            if (alive or 0) >= min_alive:
                return
            await _sleep(0.5)
        raise TimeoutError(f"the fleet never heartbeated ({min_alive} workers expected)")

    async def kill_all(self) -> None:
        for w in self.workers:
            if w.alive:
                w.sigkill()

    async def stop_graceful(self) -> None:
        for w in self.workers:
            if w.alive:
                w.terminate()
                try:
                    w.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    w.sigkill()


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


async def wait_for(
    predicate: Callable[[], Any], *, timeout_s: float, interval: float = 0.25, message: str = ""
) -> Any:
    """Poll until the predicate holds (the drills' clock)."""
    import asyncio

    deadline = time.monotonic() + timeout_s
    while True:
        result = await predicate() if _iscoroutine(predicate) else predicate()
        if result:
            return result
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting: {message or predicate}")
        await asyncio.sleep(interval)


def _iscoroutine(fn: Any) -> bool:
    import inspect

    return inspect.iscoroutinefunction(fn)


def capture(name: str, receipt: dict[str, Any]) -> Path:
    """The drill's receipt, durable (the report's evidence tier)."""
    MEASUREMENTS.mkdir(parents=True, exist_ok=True)
    path = MEASUREMENTS / f"{name}-{RUN_STAMP}.json"
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True, default=str))
    return path


def capture_append(name: str, receipt: dict[str, Any]) -> Path:
    """Multi-iteration receipts: one file, one entry per iteration."""
    MEASUREMENTS.mkdir(parents=True, exist_ok=True)
    path = MEASUREMENTS / f"{name}-{RUN_STAMP}.json"
    log = json.loads(path.read_text()) if path.exists() else []
    log.append(receipt)
    path.write_text(json.dumps(log, indent=2, sort_keys=True, default=str))
    return path


# ── the drills' row readers (the rows are the truth) ─────────────────────


async def journal_rows(
    pool: asyncpg.Pool, schema: str, *, txn_prefix: str | None = None
) -> list[asyncpg.Record]:
    """The enrichment journal (the mock API's access log, durable),
    optionally ONE batch's rows (the txn ids carry the batch id)."""
    if txn_prefix is None:
        return await pool.fetch(
            f"SELECT txn_id, call_index, outcome, latency_s, started_at, finished_at"  # noqa: S608  # Why: the schema identifier only.
            f' FROM "{schema}".lw_enrich_calls ORDER BY started_at'
        )
    return await pool.fetch(
        f"SELECT txn_id, call_index, outcome, latency_s, started_at, finished_at"  # noqa: S608  # Why: as above.
        f' FROM "{schema}".lw_enrich_calls WHERE txn_id LIKE $1 ORDER BY started_at',
        txn_prefix + "%",
    )


def journal_receipt(rows: list[asyncpg.Record]) -> dict[str, Any]:
    """The journal's receipt: the outcome histogram + the CONCURRENCY
    OVERLAP (the max number of enrichment calls in flight at once,
    measured from the rows — the fan-out's concurrency, real or not)."""
    outcomes: dict[str, int] = {}
    intervals = []
    for r in rows:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
        if r["finished_at"] is not None and r["started_at"] is not None:
            intervals.append((r["started_at"], r["finished_at"]))
    intervals.sort()
    max_in_flight = 0
    ends: list[Any] = []
    for start, finish in intervals:
        ends = [e for e in ends if e > start]
        ends.append(finish)
        max_in_flight = max(max_in_flight, len(ends))
    calls_per_txn: dict[str, int] = {}
    oks_per_txn: dict[str, int] = {}
    for r in rows:
        calls_per_txn[r["txn_id"]] = max(calls_per_txn.get(r["txn_id"], 0), r["call_index"] + 1)
        if r["outcome"] == "ok":
            oks_per_txn[r["txn_id"]] = oks_per_txn.get(r["txn_id"], 0) + 1
    return {
        "outcomes": outcomes,
        "total_calls": len(rows),
        "max_in_flight": max_in_flight,
        "calls_per_txn": calls_per_txn,
        "oks_per_txn": oks_per_txn,
    }


async def ledger_attempts(
    pool: asyncpg.Pool, schema: str, step_key: str, *, flow_id: str | None = None
) -> dict[int, int]:
    """Per map child: the attempt count (the ladder's receipt), keyed
    by the INT map_index — one FLOW's rows (the schema may carry many
    runs)."""
    if flow_id is not None:
        rows = await pool.fetch(
            f'SELECT map_index, count(*) AS attempts FROM "{schema}".wf_step_ledger'  # noqa: S608  # Why: as above.
            " WHERE step_key = $1 AND flow_id = $2 GROUP BY map_index",
            step_key,
            flow_id,
        )
    else:
        rows = await pool.fetch(
            f'SELECT map_index, count(*) AS attempts FROM "{schema}".wf_step_ledger'  # noqa: S608  # Why: as above.
            " WHERE step_key = $1 GROUP BY map_index",
            step_key,
        )
    return {int(r["map_index"]): r["attempts"] for r in rows}


async def child_jobs(
    pool: asyncpg.Pool, schema: str, step_key: str, *, flow_id: str | None = None
) -> dict[str, int]:
    """The job-row status histogram for one step key (or a prefix) —
    one FLOW's rows when the flow id is given."""
    op, key = ("LIKE", f"{step_key}%") if step_key.endswith("%") else ("=", step_key)
    if flow_id is not None:
        rows = await pool.fetch(
            f'SELECT status, count(*) AS n FROM "{schema}".jobs'  # noqa: S608  # Why: as above.
            f" WHERE step_key {op} $1 AND metadata->>'flow_id' = $2 GROUP BY status",
            key,
            flow_id,
        )
    else:
        rows = await pool.fetch(
            f'SELECT status, count(*) AS n FROM "{schema}".jobs'  # noqa: S608  # Why: as above.
            f" WHERE step_key {op} $1 GROUP BY status",
            key,
        )
    return {r["status"]: r["n"] for r in rows}


async def review_run_ids(
    pool: asyncpg.Pool, schema: str, *, batch_id: str | None = None
) -> list[str]:
    """The txn_review runs (optionally ONE batch's — the run's own
    input carries the batch id)."""
    if batch_id is None:
        rows = await pool.fetch(
            f'SELECT id FROM "{schema}".jobs'  # noqa: S608  # Why: as above.
            " WHERE step_key = '__flow__' AND metadata->>'workflow' = 'txn_review' ORDER BY id"
        )
    else:
        rows = await pool.fetch(
            f'SELECT id FROM "{schema}".jobs'  # noqa: S608  # Why: as above.
            " WHERE step_key = '__flow__' AND metadata->>'workflow' = 'txn_review'"
            " AND payload->'wf_input'->>'batch_id' = $1 ORDER BY id",
            batch_id,
        )
    return [str(r["id"]) for r in rows]
