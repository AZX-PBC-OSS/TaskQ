"""The gauge-fleet drill harness.

Runs the recurring-refresh flow against a real Postgres and CAPTURES the
receipts: every claim is measured, not asserted. Requires a Postgres reachable
at ``TASKQ_PG_DSN`` (or ``--dsn``); TaskQ's schema migrations are applied and
the example's own tables are (re)created by the harness.

    uv run python -m examples.gaugefleet.run_demo --drill all

Drills (each writes PASS/FAIL receipts into ``.measurements/probe-dash/``):

``multicycle``   five consecutive ticks driven by the chain itself — per-cycle
                 typed receipts (cursor advanced, increment processed, counts
                 exact), the wide-increment fan-out arm, per-phase progress
                 pushes (a live stream on cycle 1, the durable progress
                 snapshot for the rest), and the terminal admin view.
``overlap``      a slow cycle is running; latecomers (the chain's own successor
                 AND a manual trigger) PARK, never stack; an idle-time trigger
                 is admitted; a double-trigger of the singleton escalation
                 actor is refused with ``SingletonCollisionError``.
``cursor``       SIGKILL the worker mid-increment; the uncommitted attempt
                 vanishes, the chain recovers on the durable timer, the cursor
                 never rewinds, and nothing is digested twice.
``timer``        the approval window is a ``scheduled_at`` job: it sits HELD in
                 ``scheduled`` across a worker death, fires NO EARLIER than its
                 scheduled instant, and — unapproved — runs the EXPIRED arm:
                 a durable escalation record plus the typed fail-close
                 (:class:`~examples.gaugefleet.actors.ApprovalWindowExpired`
                 terminal at attempt one). The control arm approves in time.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

from examples.gaugefleet import actors, state
from taskq import JobFilter, TaskQ
from taskq.migrate import apply_pending_locked
from taskq.settings import TaskQSettings

REPO_ROOT = Path(__file__).resolve().parents[2]
CAPTURE_DIR = REPO_ROOT / ".measurements" / "probe-dash"

#: Per-drill tick-index namespaces keep every idempotency key globally unique
#: (the jobs table outlives the example's own table resets).
MULTI, OVERLAP, CURSOR, TIMER, TIMER2 = 100, 200, 300, 400, 500

TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


def now_s() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class Recorder:
    """Writes a drill's receipts to stdout and its capture file at once."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lines: list[str] = []
        self.failures: list[str] = []

    def log(self, msg: str) -> None:
        line = f"[{now_s()}] {msg}"
        self.lines.append(line)
        print(line, flush=True)

    def check(self, ok: bool, claim: str) -> None:
        mark = "PASS" if ok else "FAIL"
        self.log(f"  {mark}: {claim}")
        if not ok:
            self.failures.append(claim)

    async def section(self, title: str) -> None:
        self.log("")
        self.log(f"== {title} ==")

    def finish(self, name: str) -> bool:
        ok = not self.failures
        self.log("")
        self.log(f"DRILL {name}: {'PASS' if ok else 'FAIL'} ({len(self.failures)} failed claim(s))")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("\n".join(self.lines) + "\n")
        return ok


class Worker:
    """A real ``taskq worker`` subprocess the harness can kill."""

    def __init__(self, name: str, env: dict[str, str]) -> None:
        self.name = name
        self.env = env
        self.proc: subprocess.Popen[bytes] | None = None
        self.log_path = CAPTURE_DIR / f"worker-{name}.log"

    def start(self) -> None:
        taskq_bin = Path(sys.executable).parent / "taskq"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("ab") as out:
            out.write(f"\n===== worker {self.name} started {now_s()} =====\n".encode())
            out.flush()
            # Why noqa S603: the binary is the repo venv's own taskq CLI, not input.
            self.proc = subprocess.Popen(  # noqa: S603
                [
                    str(taskq_bin),
                    "worker",
                    "--actors",
                    "examples.gaugefleet.actors:registry",
                    "--queues",
                    "gaugefleet",
                ],
                cwd=REPO_ROOT,
                env=self.env,
                stdout=out,
                stderr=subprocess.STDOUT,
            )

    def sigkill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
            with self.log_path.open("ab") as f:
                f.write(f"[harness {now_s()}] SIGKILLed\n".encode())


async def wait_until(
    fn: Callable[[], Awaitable[Any | None]],
    timeout_s: float,
    desc: str,
    poll_s: float = 0.2,
) -> Any:
    """Poll until ``fn`` returns a truthy value (or the timeout aborts)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        val = await fn()
        if val:
            return val
        await asyncio.sleep(poll_s)
    raise TimeoutError(f"timed out after {timeout_s}s waiting for: {desc}")


class Harness:
    def __init__(self, dsn: str) -> None:
        # Per-run key namespace: every idempotency key embeds it, so re-running
        # the harness never dedups against a previous run's succeeded jobs.
        # Why noqa TID251: this is a harness KEY fragment, not a TaskQ row id —
        # B-tree locality is irrelevant.
        self.ns = f"r{os.getpid():x}-{uuid.uuid4().hex[:8]}"  # noqa: TID251
        self.dsn = dsn
        self.settings = TaskQSettings.load()
        self._pool: asyncpg.Pool | None = None
        self._tq: TaskQ | None = None
        self.worker: Worker | None = None
        # Fast-failover worker config for the drills. Three documented
        # invariants must hold together:
        # - the lock lease covers the failed-beat cascade: max(2,1) + 3x(2+1)
        #   = 11s <= LOCK_LEASE 15s;
        # - the grace cascade fits inside the lock lease:
        #   cancellation 5 + cleanup 2 = 7 < 15, and the event-loop watchdog
        #   budget 10 + heartbeat 2 = 12 < 15;
        # - the LEADER lease bounds failover (configuration.md's
        #   TASKQ_LEADER_LEASE: another worker takes leadership once it
        #   lapses) — 15s so a restarted worker promotes scheduled jobs fast.
        self.env = dict(os.environ) | {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": str(self.settings.schema_name),
            "TASKQ_HEARTBEAT_INTERVAL": "2",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "1",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "2",
            "TASKQ_LOCK_LEASE": "15",
            "TASKQ_LEADER_LEASE": "15",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "5",
            "TASKQ_CLEANUP_GRACE_PERIOD": "2",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "10",
        }

    @property
    def db(self) -> asyncpg.Pool:
        assert self._pool is not None, "harness not started"
        return self._pool

    @property
    def client(self) -> TaskQ:
        assert self._tq is not None, "harness not started"
        return self._tq

    async def start(self) -> None:
        """Apply TaskQ's migrations, recreate the example's tables, open the
        client facade, and start one worker."""
        conn = await asyncpg.connect(self.dsn)
        try:
            await apply_pending_locked(conn=conn, schema=str(self.settings.schema_name))
        finally:
            await conn.close()
        self._pool = await asyncpg.create_pool(self.dsn)
        await state.reset_schema(self.db)
        self._tq = TaskQ(dsn=self.dsn)
        await self._tq.__aenter__()
        self.started_at = datetime.now(UTC)
        self.worker = Worker("main", self.env)
        self.worker.start()
        await wait_until(self._worker_registered, 30, "worker registration")
        await wait_until(self._leader_live, 90, "this drill's worker holds the leadership")

    async def stop(self) -> None:
        if self.worker and self.worker.proc and self.worker.proc.poll() is None:
            self.worker.sigkill()
        if self._tq is not None:
            await self._tq.__aexit__(None, None, None)
            self._tq = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _worker_registered(self) -> Any:
        schema = self.settings.schema_name
        exists = await self.db.fetchval(f"SELECT to_regclass('{schema}.workers') IS NOT NULL")
        if not exists:
            await asyncio.sleep(2)
            return None
        return await self.db.fetchval(
            # Why noqa S608: the schema name comes from trusted TaskQ settings.
            f"SELECT 1 FROM {schema}.workers LIMIT 1"  # noqa: S608
        )

    async def _leader_live(self) -> Any:
        """True when THIS drill's worker holds the leadership.

        Scheduled-job promotion runs on the leader. A worker started right
        after a SIGKILL waits out the dead leader's lock lease before it can
        take over, and until then ``scheduled_at`` jobs are not promoted — so
        the drills must not seed scheduled work before the takeover lands.
        """
        schema = self.settings.schema_name
        exists = await self.db.fetchval(
            f"SELECT to_regclass('{schema}.maintenance_leader') IS NOT NULL"
        )
        if not exists:
            return None
        return await self.db.fetchval(
            "SELECT 1 FROM taskq.maintenance_leader l "
            "JOIN taskq.workers w ON w.id = l.worker_id "
            "WHERE w.started_at >= $1::timestamptz "
            "  AND w.last_seen_at > now() - interval '10 seconds'",
            self.started_at,
        )

    # ---- the operator surfaces (the admin view) -------------------------

    async def jobs_view(self, actor_name: str) -> list[dict[str, Any]]:
        """List a run's jobs: the docs' ``client.list(JobFilter)`` when the
        facade exposes it, the same columns read via SQL otherwise."""
        client = self.client
        list_fn: Callable[..., Any] | None = None
        candidate = getattr(client, "list", None)
        if candidate is not None:
            list_fn = candidate
        else:
            jobs_obj = getattr(client, "jobs", None)
            if jobs_obj is not None and hasattr(jobs_obj, "list"):
                list_fn = jobs_obj.list
        if list_fn is not None:
            page = await list_fn(JobFilter(actor=actor_name, limit=100))
            rows = getattr(page, "jobs", None) or getattr(page, "rows", [])
            return [
                {
                    "job_id": str(getattr(r, "job_id", "") or getattr(r, "id", "")),
                    "actor": getattr(r, "actor", actor_name),
                    "status": getattr(r, "status", "?"),
                    "attempt": getattr(r, "attempt", None),
                    "idempotency_key": getattr(r, "idempotency_key", None),
                    "scheduled_at": fmt_dt(getattr(r, "scheduled_at", None)),
                    "started_at": fmt_dt(getattr(r, "started_at", None)),
                }
                for r in rows
            ]
        rows = await self.db.fetch(
            "SELECT id::text, actor, status, attempt, idempotency_key, "  # noqa: S608
            "       scheduled_at, started_at, finished_at, error_class "
            f"FROM {self.settings.schema_name}.jobs WHERE actor = $1 "
            "ORDER BY created_at LIMIT 200",
            actor_name,
        )
        return [dict(r) for r in rows]

    async def job_row(self, job_id: str) -> dict[str, Any]:
        r = await self.db.fetchrow(
            "SELECT id::text, actor, status, attempt, idempotency_key, tags, "  # noqa: S608
            "       scheduled_at, started_at, finished_at, error_class, error_message, "
            "       progress_seq, progress_state "
            f"FROM {self.settings.schema_name}.jobs WHERE id = $1::uuid",
            job_id,
        )
        return dict(r) if r else {}

    async def cycle_row(self, tick_index: int) -> dict[str, Any] | None:
        r = await self.db.fetchrow(
            "SELECT * FROM gauge_cycle_log WHERE tick_index = $1", tick_index
        )
        return dict(r) if r else None

    def kill_worker(self) -> None:
        assert self.worker is not None, "no worker to kill"
        self.worker.sigkill()


def fmt_dt(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


async def quiesce(h: Harness, rec: Recorder) -> None:
    """Between drills: stop the worker, cancel leftover active jobs, reset the
    example's tables so the next drill starts from a clean cycle."""
    if h.worker:
        h.worker.sigkill()
    cancelled: set[str] = set()
    for actor_name in ("gauge_refresh_tick", "gauge_refresh_gauge", "gauge_approval_escalate"):
        try:
            rows = await h.jobs_view(actor_name)
        except Exception as exc:
            rec.log(f"  quiesce: job view unavailable for {actor_name}: {exc!r}")
            continue
        for row in rows:
            if row.get("status") not in ("pending", "scheduled", "running"):
                continue
            job_id = row.get("job_id")
            try:
                cancel_where = getattr(h.client, "cancel_where", None)
                if cancel_where is not None:
                    await cancel_where(JobFilter(actor=actor_name, unfinished=True))
                    cancelled = {"(bulk cancel_where)"}
                    break
                await h.client.cancel(job_id)  # type: ignore[attr-defined]
                cancelled.add(row.get("idempotency_key") or job_id)
            except Exception as exc:
                rec.log(f"  quiesce: cancel of {job_id} failed: {exc!r}")
    rec.log(f"  quiesce: cancelled leftover active jobs: {sorted(cancelled) or 'none'}")
    await state.reset_schema(h.db)


# ===========================================================================
# Drills
# ===========================================================================


async def drill_multicycle(h: Harness, rec: Recorder) -> None:
    """Five consecutive ticks on the self-scheduling chain: per-cycle typed
    receipts, the fan-out arm, the progress pushes, the terminal admin view."""
    await rec.section("MULTI-CYCLE: five consecutive ticks, exact state after each")

    live_events: list[str] = []
    stream_task: asyncio.Task[None] | None = None
    seed_handle: Any = None
    expected: dict[str, int] = {}  # gauge_id -> tick_index that digested it
    prev_cursor_after: str | None = None
    plan = {1: 3, 2: 3, 3: 3, 4: 3, 5: 20}  # cycle -> injected gauge count (20 = wide)

    for cycle, count in plan.items():
        tick_index = MULTI + cycle
        gauges = [f"gen-{MULTI + cycle}-{chr(ord('a') + i)}" for i in range(count)]
        readings = [(g, 100.0 + cycle * 10.0 + i) for i, g in enumerate(gauges)]
        await state.inject_readings(h.db, readings)

        if cycle == 1:
            seed_handle = await h.client.enqueue(
                actors.refresh_tick,
                actors.TickPayload(tick_index=tick_index, run_ns=h.ns, work_seconds=1.0),
                idempotency_key=f"gaugefleet-tick:{h.ns}:{tick_index}",
                tags=["gaugefleet", f"cycle-{tick_index}"],
            )
            rec.log(f"seeded tick {tick_index} (the chain schedules its own successors)")

            async def collect_stream(h: Any = seed_handle) -> None:
                async for ev in h.progress_stream():
                    live_events.append(
                        f"seq={ev.seq} step={ev.step} percent={ev.percent} "
                        f"status={ev.status} detail={ev.detail!r}"
                    )

            stream_task = asyncio.create_task(collect_stream())

        row = await wait_until(
            lambda ti=tick_index: h.cycle_row(ti), 40, f"tick {tick_index} receipt row"
        )
        rec.log(
            f"cycle {cycle}: tick {tick_index} outcome={row['outcome']} "
            f"processed={row['processed']} fan_out={row['fan_out']}"
        )

        wide = count > state.FAN_OUT_THRESHOLD
        rec.check(row["outcome"] == "succeeded", f"cycle {cycle}: outcome == succeeded")
        rec.check(
            row["processed"] == (0 if wide else count) and row["fan_out"] == (count if wide else 0),
            f"cycle {cycle}: counts exact (processed={row['processed']}, "
            f"fan_out={row['fan_out']}, injected={count})",
        )
        rec.check(
            sorted(json.loads(row["gauge_ids"])) == sorted(gauges),
            f"cycle {cycle}: the increment was exactly the injected gauges",
        )
        if prev_cursor_after is not None:
            rec.check(
                str(row["cursor_before"]) >= prev_cursor_after,
                f"cycle {cycle}: cursor never rewound "
                f"(before={row['cursor_before']} >= previous after={prev_cursor_after})",
            )
        prev_cursor_after = str(row["cursor_after"])
        for g in gauges:
            expected[g] = tick_index

        if not wide:
            digest = {d["gauge_id"]: d["digested_by_tick"] for d in await state.digest_rows(h.db)}
            rec.check(
                all(digest.get(g) == expected[g] for g in expected),
                f"cycle {cycle}: digest state exact ({len(expected)} gauges, "
                f"each digested by its own cycle)",
            )
        else:
            # The fan-out children digest asynchronously: wait for all of them.
            async def _children_done(gset: frozenset[str] = frozenset(gauges)) -> Any:
                digest = await state.digest_rows(h.db)
                done = [d for d in digest if d["gauge_id"] in gset]
                return done if len(done) == len(gset) else None

            await wait_until(_children_done, 30, f"cycle {cycle}: fan-out children done")
            digest = {d["gauge_id"]: d["digested_by_tick"] for d in await state.digest_rows(h.db)}
            rec.check(
                all(digest.get(g) == tick_index for g in gauges),
                f"cycle {cycle}: all {count} fan-out children landed in the digest",
            )

        # The durable progress snapshot for this tick's job (cycle 1's live
        # stream is reported separately below).
        job = await h.job_row(str(row["job_id"]))
        rec.log(
            f"cycle {cycle}: progress_seq={job.get('progress_seq')} "
            f"progress_state={job.get('progress_state')}"
        )
        rec.check(
            job.get("status") == "succeeded",
            f"cycle {cycle}: SUCCESS receipt — the tick's job row is terminal-succeeded",
        )

    rec.check(
        not seed_handle.was_existing,
        "the seed enqueue was not a dedup hit (a fresh run starts a fresh chain)",
    )
    if stream_task is not None:
        await asyncio.wait_for(stream_task, timeout=10)
    rec.log("cycle 1 live progress events (JobHandle.progress_stream()):")
    for line in live_events:
        rec.log(f"  {line}")
    rec.check(
        any("percent=100" in e for e in live_events) and len(live_events) >= 2,
        "cycle 1: the refresh pushed observable progress while it ran "
        "(>=2 events, terminal at 100%)",
    )

    # The terminal ADMIN VIEW: every cycle's tick, its status, its identity.
    view = await h.jobs_view("gauge_refresh_tick")
    plan_keys = {f"gaugefleet-tick:{h.ns}:{MULTI + c}" for c in plan}
    cycle_view = [r for r in view if r.get("idempotency_key") in plan_keys]
    rec.log("terminal admin view (the run's cycle ticks, listed by the client API):")
    for r in view:
        rec.log(
            f"  job {r['job_id']} status={r['status']} attempt={r.get('attempt')} "
            f"key={r.get('idempotency_key')} scheduled={r.get('scheduled_at')}"
        )
    rec.check(
        len(cycle_view) >= 5 and all(r["status"] == "succeeded" for r in cycle_view),
        "terminal admin view: every cycle's tick is listed and succeeded",
    )
    (CAPTURE_DIR / "admin-view-terminal.json").write_text(json.dumps(view, indent=2, default=str))


async def drill_overlap(h: Harness, rec: Recorder) -> None:
    """A slow cycle is running: latecomers park, an idle-time trigger is
    admitted, and a singleton double-trigger is refused."""
    await rec.section("OVERLAP GUARD: a slow tick must not stack")

    await state.inject_readings(h.db, [("ov-a", 10.0), ("ov-b", 20.0)])
    handle = await h.client.enqueue(
        actors.refresh_tick,
        actors.TickPayload(tick_index=OVERLAP + 1, run_ns=h.ns, work_seconds=25.0),
        idempotency_key=f"gaugefleet-tick:{h.ns}:{OVERLAP + 1}",
        tags=["gaugefleet", f"cycle-{OVERLAP + 1}"],
    )
    await wait_until(lambda: _guard_running(h), 30, "the slow tick claimed the guard")
    guard = await state.guard_row(h.db)
    rec.check(
        guard["status"] == "running",
        f"the guard row shows the cycle RUNNING (status={guard['status']}, "
        f"claimed_at={guard['claimed_at']})",
    )

    # Latecomer 1: the chain's own successor, due while the slow tick still runs.
    row_succ = await wait_until(
        lambda: h.cycle_row(OVERLAP + 2), 30, "the successor's parked receipt"
    )
    rec.check(
        row_succ["outcome"] == "parked",
        "the chain's own successor PARKED (outcome=parked), not stacked",
    )
    guard = await state.guard_row(h.db)
    rec.check(
        guard["status"] == "running" and guard["run_token"] is not None,
        "the parked successor left the running cycle's guard claim untouched",
    )

    # Latecomer 2: a manual trigger while the cycle runs.
    manual = await h.client.enqueue(
        actors.refresh_tick,
        actors.TickPayload(tick_index=901, run_ns=h.ns),
        idempotency_key=f"gaugefleet-tick:{h.ns}:manual-901",
    )
    rec.log(f"manual trigger enqueued while a cycle runs (job {manual.job_id})")
    row_m = await wait_until(lambda: h.cycle_row(901), 30, "the manual tick's parked receipt")
    rec.check(
        row_m["outcome"] == "parked",
        "the manual trigger PARKED (refused/parked, skip-not-raise) — measured",
    )

    result = await handle.wait(timeout=60)
    rec.log(f"slow tick finished: {result.model_dump()}")
    rec.check(
        result.outcome == "succeeded" and result.processed == 2,
        "the slow cycle completed exactly (processed=2)",
    )

    # Idle-time trigger: admitted the moment the guard is free. Retry a couple
    # of times — another chain successor may briefly hold the guard first.
    admitted = None
    for attempt in (902, 903, 904):
        await h.client.enqueue(
            actors.refresh_tick,
            actors.TickPayload(tick_index=attempt, run_ns=h.ns),
            idempotency_key=f"gaugefleet-tick:{h.ns}:manual-{attempt}",
        )
        row_i = await wait_until(
            lambda ti=attempt: h.cycle_row(ti), 20, f"manual tick {attempt} receipt"
        )
        if row_i["outcome"] == "succeeded":
            admitted = row_i
            break
    rec.check(
        admitted is not None and admitted["processed"] == 0,
        "an idle-time manual trigger is ADMITTED (empty increment, processed=0)",
    )

    # The singleton face on the escalation actor (which never self-enqueues):
    # a second active-job enqueue is REFUSED with the typed error.
    breach = f"breach-{h.ns}-ov-singleton"
    await state.record_breach(h.db, breach, "ov-singleton", 999.0)
    await h.client.enqueue(
        actors.approval_escalate,
        actors.EscalatePayload(breach_id=breach),
        scheduled_at=datetime.now(UTC) + timedelta(seconds=30),
        idempotency_key=f"gaugefleet-approval:{breach}",
    )
    refused = False
    try:
        await h.client.enqueue(actors.approval_escalate, actors.EscalatePayload(breach_id=breach))
    except Exception as exc:
        refused = type(exc).__name__ == "SingletonCollisionError"
        rec.log(f"double-trigger refused with: {type(exc).__name__}: {exc}")
    rec.check(
        refused,
        "double-trigger of the singleton actor refused with SingletonCollisionError",
    )


async def drill_cursor(h: Harness, rec: Recorder) -> None:
    """SIGKILL mid-increment: the uncommitted attempt vanishes, the chain
    recovers on the durable timer, the cursor never rewinds, nothing is
    digested twice."""
    await rec.section("CURSOR PROOF: kill mid-increment, restart, no rewind")

    gauges = [f"cur-{i}" for i in range(3)]
    await state.inject_readings(h.db, [(g, 50.0 + i) for i, g in enumerate(gauges)])
    cursor0 = await state.read_cursor(h.db)

    await h.client.enqueue(
        actors.refresh_tick,
        actors.TickPayload(tick_index=CURSOR + 1, run_ns=h.ns, work_seconds=6.0),
        idempotency_key=f"gaugefleet-tick:{h.ns}:{CURSOR + 1}",
        tags=["gaugefleet", f"cycle-{CURSOR + 1}"],
    )
    await wait_until(lambda: _guard_running(h), 30, "the doomed tick claimed the guard")
    rec.log(f"SIGKILLing the worker mid-increment (tick {CURSOR + 1} is mid-work)")
    h.kill_worker()

    # Nothing was committed: the cursor did not move, the digest is untouched,
    # and the killed attempt's receipt row does not exist.
    cursor1 = await state.read_cursor(h.db)
    digest1 = await state.digest_rows(h.db)
    rec.check(
        cursor1 == cursor0,
        f"mid-kill: the durable cursor did NOT move (still {cursor0})",
    )
    rec.check(not digest1, "mid-kill: the digest holds nothing from the killed attempt")
    rec.check(
        await h.cycle_row(CURSOR + 1) is None,
        "mid-kill: the killed attempt's receipt row does not exist (atomic commit)",
    )

    # Restart: the chain's successor was scheduled BEFORE the kill (the clock
    # held the door); after the stale-guard window the chain resumes and the
    # increment is processed exactly once.
    h.worker = Worker("restarted", h.env)
    h.worker.start()
    rec.log("restarted the worker; waiting for the chain to recover the increment")

    async def _recovered() -> Any:
        digest = await state.digest_rows(h.db)
        got = {d["gauge_id"] for d in digest}
        return digest if got >= set(gauges) else None

    digest = await wait_until(_recovered, 60, "the increment fully digested after restart")

    rows = await state.cycle_rows(h.db)
    rec.log("cycle log after recovery:")
    for r in rows:
        rec.log(
            f"  tick {r['tick_index']} attempt {r['attempt']} {r['outcome']} "
            f"processed={r['processed']} before={r['cursor_before']} after={r['cursor_after']}"
        )
    rec.check(
        all(r["cursor_after"] is None or r["cursor_after"] >= r["cursor_before"] for r in rows),
        "every committed receipt shows a non-rewinding cursor",
    )
    digested_by = {d["gauge_id"]: d["digested_by_tick"] for d in digest}
    rec.check(
        all(digested_by[g] != CURSOR + 1 for g in gauges),
        "the increment was digested exactly once, by the RECOVERY tick "
        f"(digested_by_tick={ {g: digested_by[g] for g in gauges} })",
    )
    total = len(digest)
    rec.check(
        total == len(gauges),
        f"the digest holds exactly {len(gauges)} rows — no reprocessing, no duplicates",
    )

    # The cursor sits past every committed reading, so ANY re-attempt of the
    # killed job (the library's reclaim will eventually re-run it) resumes to
    # an empty increment: resume-not-restart.
    cursor_now = await state.read_cursor(h.db)
    increment_now = await state.fetch_increment(h.db, cursor_now)
    rec.check(
        not increment_now,
        "the cursor sits past every committed reading: a re-attempt of the "
        "killed job resumes to an EMPTY increment (nothing to reprocess)",
    )


async def drill_timer(h: Harness, rec: Recorder) -> None:
    """The approval window is a scheduled_at job: HELD across a worker death,
    fires no earlier than scheduled, and the EXPIRED arm fails close."""
    await rec.section("TIMER + ESCALATION: the clock holds the door")

    # --- control arm: approved within the window -> the success arm runs ---
    await state.inject_readings(h.db, [("tim-ok", 950.0)])
    await h.client.enqueue(
        actors.refresh_tick,
        actors.TickPayload(tick_index=TIMER + 1, run_ns=h.ns, work_seconds=0.5),
        idempotency_key=f"gaugefleet-tick:{h.ns}:{TIMER + 1}",
        tags=["gaugefleet", f"cycle-{TIMER + 1}"],
    )
    await wait_until(lambda: h.cycle_row(TIMER + 1), 30, "the control tick's receipt")
    breach = await state.breach_row(h.db, f"breach-{h.ns}-tim-ok")
    rec.check(bool(breach), "the control tick recorded the threshold breach")
    esc_job = await wait_until(
        lambda: _job_by_key(h, f"gaugefleet-approval:breach-{h.ns}-tim-ok"),
        20,
        "the escalation job exists (the HITL timer arm)",
    )
    rec.check(
        esc_job["status"] == "scheduled" and esc_job["scheduled_at"] is not None,
        f"the escalation job sits HELD in scheduled "
        f"(scheduled_at={esc_job['scheduled_at']}) — the operator's held-timer view",
    )
    await state.approve(h.db, f"breach-{h.ns}-tim-ok")
    rec.log("the operator approved the breach within its window")
    await wait_until(
        lambda: _job_terminal(h, str(esc_job["id"])),
        60,
        "the control escalation reached a terminal state",
    )
    esc_after = await _job_by_key(h, f"gaugefleet-approval:breach-{h.ns}-tim-ok")
    assert esc_after is not None
    esc_job = esc_after
    rec.check(
        esc_job["status"] == "succeeded",
        "approved in window -> the timer arm SUCCEEDED",
    )
    rec.check(
        not await _escalation_exists(h.db, f"breach-{h.ns}-tim-ok"),
        "no escalation row for the approved breach (the expired arm did not run)",
    )

    # --- fail-close arm: no approval; hold across a worker death ---
    await state.inject_readings(h.db, [("tim-bad", 999.0)])
    await h.client.enqueue(
        actors.refresh_tick,
        actors.TickPayload(tick_index=TIMER2 + 1, run_ns=h.ns, work_seconds=0.5),
        idempotency_key=f"gaugefleet-tick:{h.ns}:{TIMER2 + 1}",
        tags=["gaugefleet", f"cycle-{TIMER2 + 1}"],
    )
    await wait_until(lambda: h.cycle_row(TIMER2 + 1), 30, "the fail-close tick's receipt")
    esc = await wait_until(
        lambda: _job_by_key(h, f"gaugefleet-approval:breach-{h.ns}-tim-bad"),
        20,
        "the fail-close escalation job exists",
    )
    assert esc is not None
    rec.log(
        f"escalation job {esc['id']} HELD in scheduled "
        f"(window closes {esc['scheduled_at']}); capturing the mid-flight admin "
        "view, then killing the worker"
    )

    mid_view = await h.jobs_view("gauge_approval_escalate")
    (CAPTURE_DIR / "admin-view-midflight.json").write_text(
        json.dumps(mid_view, indent=2, default=str)
    )
    cli_out = _taskq_job_show(h, str(esc["id"]))
    (CAPTURE_DIR / "admin-view-midflight-job-show.txt").write_text(cli_out)
    rec.log(f"`taskq job show {esc['id']}` (mid-flight):\n{cli_out.strip()}")

    h.kill_worker()
    await asyncio.sleep(2.0)
    held = await _job_by_key(h, f"gaugefleet-approval:breach-{h.ns}-tim-bad")
    assert held is not None
    rec.check(
        held["status"] == "scheduled",
        "the timer SURVIVED the worker death: still held in scheduled "
        "(the clock holds the door while no worker exists)",
    )

    h.worker = Worker("restarted-timer", h.env)
    h.worker.start()
    rec.log("restarted a worker; the held timer wakes the fleet when its window closes")

    await wait_until(
        lambda: _job_terminal(h, str(esc["id"])), 60, "the escalation reached terminal"
    )
    esc_failed = await _job_by_key(h, f"gaugefleet-approval:breach-{h.ns}-tim-bad")
    assert esc_failed is not None
    esc = esc_failed
    rec.check(
        esc["started_at"] >= esc["scheduled_at"],
        f"the escalation fired NO EARLIER than scheduled "
        f"(started {esc['started_at']} >= scheduled {esc['scheduled_at']})",
    )
    rec.check(
        esc["status"] == "failed" and esc.get("error_class") == "ApprovalWindowExpiredError",
        f"the EXPIRED arm ran fail-close: terminal failed with typed "
        f"ApprovalWindowExpiredError (status={esc['status']}, "
        f"error_class={esc.get('error_class')})",
    )
    rec.check(
        esc.get("attempt") == 1,
        "the typed escalation was terminal at attempt ONE (non-retryable fail-close)",
    )
    escalation = await _escalation_exists(h.db, f"breach-{h.ns}-tim-bad")
    rec.check(
        bool(escalation),
        f"the durable escalation record was written before the raise: "
        f"{(escalation and str(escalation['detail'])[:80])!r}",
    )


# ---- small observability helpers ------------------------------------------


async def _guard_running(h: Harness) -> Any:
    row = await state.guard_row(h.db)
    return row if row.get("status") == "running" else None


async def _job_by_key(h: Harness, key: str) -> dict[str, Any] | None:
    r = await h.db.fetchrow(
        "SELECT id::text, status, attempt, scheduled_at, started_at, finished_at, "  # noqa: S608
        "       error_class, error_message "
        f"FROM {h.settings.schema_name}.jobs WHERE idempotency_key = $1",
        key,
    )
    return dict(r) if r else None


async def _job_terminal(h: Harness, job_id: str) -> dict[str, Any] | None:
    row = await h.job_row(job_id)
    return row if row.get("status") in TERMINAL else None


async def _escalation_exists(pool: asyncpg.Pool, breach_id: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM gauge_escalations WHERE breach_id = $1", breach_id)
    return dict(r) if r else None


def _taskq_job_show(h: Harness, job_id: str) -> str:
    taskq_bin = Path(sys.executable).parent / "taskq"
    # Why noqa S603: the binary is the repo venv's own taskq CLI, not input.
    proc = subprocess.run(  # noqa: S603
        [str(taskq_bin), "job", "show", job_id],
        cwd=REPO_ROOT,
        env=h.env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return proc.stdout + proc.stderr


# ===========================================================================

DRILLS: dict[str, Callable[[Harness, Recorder], Awaitable[None]]] = {
    "multicycle": drill_multicycle,
    "overlap": drill_overlap,
    "cursor": drill_cursor,
    "timer": drill_timer,
}


async def run() -> bool:
    parser = argparse.ArgumentParser(description="gauge-fleet drill harness")
    parser.add_argument("--dsn", default=state.DSN)
    parser.add_argument("--drill", default="all", choices=(*DRILLS, "all"))
    args = parser.parse_args()

    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
    results: dict[str, bool] = {}
    for name, drill in DRILLS.items():
        if args.drill not in ("all", name):
            continue
        rec = Recorder(CAPTURE_DIR / f"{name}.txt")
        h = Harness(args.dsn)
        try:
            await h.start()
            rec.log("bootstrapped: migrations applied, example tables reset, worker up")
            await drill(h, rec)
        except Exception:
            rec.log(f"DRILL {name}: ABORTED by exception")
            rec.log(traceback.format_exc())
            rec.failures.append("aborted by exception")
        finally:
            try:
                await quiesce(h, rec)
                await h.stop()
            except Exception:
                rec.log("teardown issue (state left behind):\n" + traceback.format_exc())
            results[name] = rec.finish(name)

    print("\n===== SUMMARY =====")
    all_ok = True
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")
        all_ok = all_ok and ok
    print(f"captures: {CAPTURE_DIR}/")
    return all_ok


if __name__ == "__main__":
    raise SystemExit(0 if asyncio.run(run()) else 1)
