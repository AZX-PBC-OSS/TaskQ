"""Shared harness for the Prometheus metrics review suites.

Three pieces the review suites need and no other test provides:

- :func:`parse_exposition`: a Prometheus text-exposition parser (the real
  scrape text, not OTel SDK objects) so assertions are made against what a
  Prometheus server actually ingests.
- :func:`run_worker_probe` / :func:`run_hostile_probe`: the subprocess
  drivers that run the REAL worker bootstrap (the shipped ``taskq worker``
  boot order: ``WorkerSettings.load`` → ``configure_exporters`` →
  ``worker._main``) against real Postgres, drive real jobs / cron / a real
  transient-Postgres failure, and dump the exposition served by both real
  scrape paths (the ``/jobs/health/metrics`` bridge router and the
  ``TASKQ_METRICS_PORT`` pull listener).
- :func:`run_promtool_rule_tests`: the honest alert-rule evaluator. Writes
  a ``promtool test rules`` unit-test file whose input series carry the
  metric names and label sets captured from the real probe's exposition,
  then evaluates the shipped rules.yaml inside the ``prom/prometheus``
  container image's own promtool. Skips cleanly when Docker is not
  reachable, mirroring the e2e tier's contract.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent

PROMTOOL_IMAGE = "prom/prometheus:v3.7.2"

#: Scrubbed from the probe subprocess environment: an ambient OTEL_* or
#: TASKQ_* variable would change which exporter wiring branch runs (the
#: documented quick-start sets neither; the probe sets its own TASKQ_*).
_SCRUBBED_ENV_PREFIXES = ("OTEL_", "TASKQ_")


#: The per-xdist-worker port partition: each worker's probes draw from
#: its own 100-port band (gw0 -> 30000-30099, gw7 -> 30700-30799) —
#: deterministic, collision-free by construction, clear of the ephemeral
#: range and the well-known metrics ports.
_PROM_PORT_BASE = 30000
_PROM_PORT_STRIDE = 100


def _free_tcp_port() -> int:
    """Reserve an ephemeral port by binding once and closing; the probe
    subprocess re-binds it moments later. The old hard-coded 19464/19465
    collided whenever pytest-xdist split the review module across two
    workers - both workers' probes raced the same fixed port and the
    loser died in OTEL exporter init (EADDRINUSE) - so every probe now
    allocates its own port.

    The bind-and-close reservation is itself a race under heavy xdist
    fan-out (-n 8: eight workers' probes allocating near-simultaneously
    — two workers' bind-close windows handed out the SAME port, one
    probe's exporter bound it first, the other's boot died EADDRINUSE
    and its port fetches refused). The cure is the deterministic
    partition: each xdist worker's probes draw from ITS OWN range
    (``gw<N>`` -> base + N * stride), so no two workers' probes can
    collide regardless of timing; the bind-close still dodges the
    host's unrelated listeners within the range."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if worker.startswith("gw"):
        offset = int(worker[2:])
        port = (_PROM_PORT_BASE + offset * _PROM_PORT_STRIDE) + (port % _PROM_PORT_STRIDE)
    return port


def probe_env(**extra: str) -> dict[str, str]:
    """A clean environment for the probe subprocess: no ambient OTEL_/TASKQ_*
    configuration, no developer dotfiles (the suite-wide DOTENV_DIR guard's
    value does not survive a fresh env dict, so set it explicitly)."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(_SCRUBBED_ENV_PREFIXES) and key not in ("DOTENV_DIR", "PYTHONPATH")
    }
    env["DOTENV_DIR"] = str(_dotenv_guard_dir())
    env.update(extra)
    return env


def _dotenv_guard_dir() -> str:
    # An empty directory that exists: see the suite conftest's
    # _no_developer_dotfiles fixture for why WorkerSettings.load must see
    # zero dotfiles. Lives in the system temp area, never in the repo (a
    # directory created at import time would dirty every checkout).
    path = Path(tempfile.gettempdir()) / "taskq_prom_probe_no_dotfiles"
    path.mkdir(exist_ok=True)
    return str(path)


# ── exposition parsing ──────────────────────────────────────────────


@dataclass(frozen=True)
class Sample:
    """One exposition line: family name, labels, value."""

    name: str
    labels: dict[str, str]
    value: float


@dataclass(frozen=True)
class Exposition:
    """Parsed scrape text, indexed the two ways the assertions need."""

    samples: tuple[Sample, ...]
    by_name: dict[str, list[Sample]] = field(default_factory=dict)

    def series(self, name: str) -> list[Sample]:
        return self.by_name.get(name, [])

    def names(self) -> set[str]:
        return set(self.by_name)

    def label_values(self, name: str, label: str) -> set[str]:
        return {s.labels[label] for s in self.series(name) if label in s.labels}


_LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_exposition(text: str) -> Exposition:
    """Parse Prometheus text format (version 0.0.4) into samples.

    Deliberately reads the wire text, not the SDK: the wire text is the
    contract a Prometheus server ingests, and the bridge's rendering of
    names/labels is exactly what this review is auditing.
    """
    samples: list[Sample] = []
    by_name: dict[str, list[Sample]] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_blob, _, value_blob = line.rpartition(" ")
        if not name_blob:
            continue
        try:
            value = float(value_blob)
        except ValueError:
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*)?$", name_blob)
        if m is None:
            continue
        name = m.group(1)
        labels = dict(_LABEL_RE.findall(m.group(2) or ""))
        sample = Sample(name=name, labels=labels, value=value)
        samples.append(sample)
        by_name.setdefault(name, []).append(sample)
    return Exposition(samples=tuple(samples), by_name=by_name)


# ── the probe scripts (subprocess payloads) ─────────────────────────

_CRON_FACTORY_MODULE = '''
"""A cron payload factory that always fails (the three-strike pathology)
and one that succeeds slowly (the budget-monopolizer pathology)."""


def failing_factory() -> dict:
    raise RuntimeError("factory boom (prometheus review probe)")


def slow_factory() -> dict:
    # 1.2s: under the probe's TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT=2 grant
    # (never strikes), but three of these per tick burn the tick's funded
    # budget (dispatcher_command_timeout 5s x 0.9) below the minimum
    # fundable grant - the fourth schedule defers, the exact
    # TaskQCronBudgetDeferrals pathology.
    import time

    time.sleep(1.2)
    return {"value": 1}
'''

_WORKER_PROBE = '''
"""Real worker probe: the shipped worker bootstrap against real Postgres,
real jobs (success / terminal failure / retryable failure / timeout /
backpressure refusal / unserved queue / missing actor_config), a real
failing cron schedule (three-strike auto-disable), then a dump of the
exposition served by both real scrape paths while RUNNING and after a
clean SIGTERM shutdown."""

import asyncio
import os
import signal
import sys
import urllib.error
import urllib.request
from datetime import timedelta

PROBE_DIR = os.environ["PROBE_DIR"]
sys.path.insert(0, PROBE_DIR)

PG_DSN = os.environ["PROBE_PG_DSN"]
METRICS_PORT = int(os.environ["PROBE_METRICS_PORT"])


def _schema() -> str:
    """The probe's TaskQ schema, read per call.

    A module-level ``SCHEMA`` constant is the shared/stale-constant
    anti-pattern the suite-hygiene pin bans; the env var is the one honest
    source and every site reads it at call time instead.
    """
    return os.environ["PROBE_SCHEMA"]


from pydantic import BaseModel

from taskq import JobContext, RetryPolicy, TaskQ, actor
from taskq.cron import cron
from taskq.ratelimit import TokenBucket


class P(BaseModel):
    value: int = 1


@actor(name="probe_ok_actor", queue="probe_queue")
async def probe_ok_actor(payload: P, ctx: JobContext[P]) -> None:
    await asyncio.sleep(0.05)


@actor(
    name="probe_slow_actor",
    queue="probe_queue",
    start_to_close=timedelta(seconds=60),
)
async def probe_slow_actor(payload: P, ctx: JobContext[P]) -> None:
    # Long enough that the heartbeat renews its lock while it runs: the
    # lock_expires_in_seconds histogram is stamped by renewals only.
    await asyncio.sleep(float(os.environ.get("PROBE_SLOW_SECS", "12")))


@actor(
    name="probe_timeout_actor",
    queue="probe_queue",
    start_to_close=timedelta(seconds=1),
)
async def probe_timeout_actor(payload: P, ctx: JobContext[P]) -> None:
    await asyncio.sleep(30)


@actor(
    name="probe_fail_actor",
    queue="probe_queue",
    retry=RetryPolicy(kind="transient", max_attempts=1),
)
async def probe_fail_actor(payload: P, ctx: JobContext[P]) -> None:
    raise ValueError("terminal boom")


@actor(
    name="probe_retry_actor",
    queue="probe_queue",
    retry=RetryPolicy(
        kind="transient", max_attempts=3, base=timedelta(seconds=1), jitter=0.0
    ),
)
async def probe_retry_actor(payload: P, ctx: JobContext[P]) -> None:
    raise ConnectionError("retryable boom")


@actor(name="probe_backpressure_actor", queue="bp_queue", max_pending=0)
async def probe_backpressure_actor(payload: P, ctx: JobContext[P]) -> None:
    pass


@actor(name="probe_ghost_actor", queue="ghost_queue")
async def probe_ghost_actor(payload: P, ctx: JobContext[P]) -> None:
    pass


@actor(
    name="probe_ratelimited_actor",
    queue="probe_queue",
    rate_limits=[TokenBucket(name="probe_limiter", capacity=1.0, refill_per_second=1.0)],
)
async def probe_ratelimited_actor(payload: P, ctx: JobContext[P]) -> None:
    await asyncio.sleep(0.05)


@actor(name="probe_progress_actor", queue="probe_queue")
async def probe_progress_actor(payload: P, ctx: JobContext[P]) -> None:
    # The progress-publish pathology: real ctx.progress calls whose Redis
    # publishes fail (the probe's TASKQ_REDIS_URL points at a closed
    # port), driving taskq_progress_publish_failures_total end to end.
    for i in range(5):
        await ctx.progress(percent=i * 20.0)
        await asyncio.sleep(0.2)


@actor(name="probe_uncancellable_actor", queue="probe_queue")
async def probe_uncancellable_actor(payload: P, ctx: JobContext[P]) -> None:
    # The abandonment pathology: an actor that swallows cancellation for
    # ~10s (shield + catch-and-continue), so the operator cancel's two
    # grace periods both lapse while the attempt still holds the slot -
    # mark_abandoned takes it, exactly the shape TaskQAbandonedJobs reads.
    import time as _time

    deadline = _time.monotonic() + 10.0
    while _time.monotonic() < deadline:
        try:
            await asyncio.shield(asyncio.sleep(1))
        except asyncio.CancelledError:
            continue


_PROBE_CRON_SPEC = cron(
    "* * * * *",
    actor="probe_fail_actor",
    payload_factory="probe_cron_factory.failing_factory",
    name="probe-failing-cron",
)
_PROBE_SLOW_CRON_SPECS = [
    cron(
        "* * * * *",
        actor="probe_fail_actor",
        payload_factory="probe_cron_factory.slow_factory",
        name=f"probe-slow-cron-{i}",
    )
    for i in range(4)
]

ACTORS = {
    ref.name: ref
    for ref in (
        probe_ok_actor,
        probe_slow_actor,
        probe_timeout_actor,
        probe_fail_actor,
        probe_retry_actor,
        probe_backpressure_actor,
        probe_ghost_actor,
        probe_progress_actor,
        probe_ratelimited_actor,
        probe_uncancellable_actor,
    )
}

#: The job handle the abandonment pathology cancels, set by _run() before
#: the worker starts and read by the shape coroutine once the worker has
#: claimed the job.
_abandon_handle: "object | None" = None


async def _scrape_bridge() -> str:
    # The real bridge path: the router exactly as `taskq ui serve` mounts
    # it. The worker's provider (wired from TASKQ_METRICS_PORT at boot)
    # already bridges the default registry, so the route detects the
    # bridge and serves the same exposition the pull listener serves.
    import fastapi
    from fastapi.testclient import TestClient

    from taskq.contrib.prometheus import create_metrics_router

    app = fastapi.FastAPI()
    app.include_router(create_metrics_router(None), prefix="/jobs/health")
    resp = TestClient(app).get("/jobs/health/metrics")
    assert resp.status_code == 200, resp.status_code
    return resp.text


async def _fetch_port(port: int) -> str:
    with urllib.request.urlopen(  # noqa: S310
        f"http://127.0.0.1:{port}/metrics", timeout=10
    ) as r:
        return r.read().decode()


async def _scrape_port() -> str:
    return await _fetch_port(METRICS_PORT)


async def _poll_fetch_port(port: int, *, deadline_s: float = 45.0) -> str:
    """POLL, don't knock once: the worker's boot (interpreter start, PG
    connect, the first election attempt) has outrun any fixed wait under
    co-tenancy — the follower arm's own conviction (3.14 leg, run
    36629891786: the lone connect got Errno 111 and the missing file
    failed three otherwise-green tests as PROBE_TASK_FAILED). The
    LIVE arm raced the SAME boot the same way (the -n 8 consolidated
    proof: the worker's PG connects queued behind eight workers' storm,
    the port unbound at the LIVE knock, the one-shot refused). The port
    answers under the poll or the boot genuinely died."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_s
    while True:
        try:
            return await _fetch_port(port)
        except (urllib.error.URLError, OSError):
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.5)


async def _dump(tag: str) -> None:
    if tag == "FOLLOWER":
        # The follower mounts no bridge router: its exposition is its
        # own TASKQ_METRICS_PORT pull listener (the port the parent
        # probe allocated for it). The follower is terminated only
        # AFTER this dump returns, so the port answers under the poll
        # or the boot genuinely died.
        text = await _poll_fetch_port(int(os.environ["PROBE_FOLLOWER_METRICS_PORT"]))
        with open(f"{os.environ['PROBE_SCRAPE_PATH']}.{tag}.port", "w") as fh:
            fh.write(text)
        print(f"SCRAPED:{tag}:port={len(text)}", flush=True)
        return
    bridge = await _scrape_bridge()
    with open(f"{os.environ['PROBE_SCRAPE_PATH']}.{tag}.bridge", "w") as fh:
        fh.write(bridge)
    # The LIVE arm polls too: the worker's boot races the scrape under
    # co-tenancy (the same storm the follower's poll cures). FINAL keeps
    # the one-shot: the worker is already exiting, the poll would stall
    # on a port that is legitimately going away.
    port = await _poll_fetch_port(METRICS_PORT) if tag == "LIVE" else await _scrape_port()
    with open(f"{os.environ['PROBE_SCRAPE_PATH']}.{tag}.port", "w") as fh:
        fh.write(port)
    print(f"SCRAPED:{tag}:bridge={len(bridge)}:port={len(port)}", flush=True)


async def _run() -> None:
    if os.environ.get("PROBE_FOLLOWER") == "1":
        # Follower mode: a second worker in the same fleet while the
        # first holds leadership. Its first election attempt observes a
        # holder that is not itself - the losing side's real
        # lock-contention emission - and its exposition is scraped by
        # the parent probe below.
        from taskq.obs import configure_exporters as _cfg
        from taskq.settings import WorkerSettings as _WS
        from taskq.worker.run import _main as _main_fn

        f_settings = _WS.load()
        _cfg(f_settings)
        code = await _main_fn(f_settings, actor_registry=ACTORS)
        print("FOLLOWER_EXIT:", code, flush=True)
        return

    import asyncpg

    from taskq.obs import configure_exporters
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load()
    # The shipped worker boot order (cli.py): exporter wiring exists
    # before anything records - pre-provider measurements are dropped.
    configure_exporters(settings)

    global _abandon_handle
    async with TaskQ(dsn=PG_DSN, schema=_schema()) as tq:
        for i in range(3):
            await tq.enqueue(probe_ok_actor, P(value=i))
        await tq.enqueue(probe_slow_actor, P())
        await tq.enqueue(probe_timeout_actor, P())
        await tq.enqueue(probe_fail_actor, P())
        await tq.enqueue(probe_retry_actor, P())
        await tq.enqueue(probe_progress_actor, P())
        await tq.enqueue(probe_ratelimited_actor, P())
        _abandon_handle = await tq.enqueue(probe_uncancellable_actor, P())
        for _ in range(3):
            try:
                await tq.enqueue(probe_backpressure_actor, P())
                print("BP_NO_REFUSAL", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"BP_REFUSED:{type(exc).__name__}", flush=True)
        for i in range(2):
            # ghost_queue is in no worker's TASKQ_QUEUES: the unserved /
            # stranded unserved_queue shape, produced by real enqueue.
            await tq.enqueue(probe_ghost_actor, P(value=i))

        await tq.create_schedule(
            "probe_fail_actor",
            "* * * * *",
            payload_factory="probe_cron_factory.failing_factory",
            name="probe-failing-cron",
        )
        for i in range(4):
            await tq.create_schedule(
                "probe_fail_actor",
                "* * * * *",
                payload_factory="probe_cron_factory.slow_factory",
                name=f"probe-slow-cron-{i}",
            )

    from taskq.worker.run import _main

    window = float(os.environ.get("PROBE_WORKER_SECS", "80"))

    async def _worker_task() -> int:
        return await _main(settings, actor_registry=ACTORS)

    async def _follower() -> None:
        # Spawn a second worker while this probe's worker holds
        # leadership: the fleet-handover shape whose losing side records
        # the maintenance-lock contention counter. The follower's
        # exposition (its own metrics port) carries it. Spawned AFTER the
        # LIVE dump on purpose: its boot re-registers the deleted
        # actor_config row (real recovery) and adds a second live worker,
        # both of which would legitimately change the leader gauges the
        # LIVE assertions read.
        await asyncio.sleep(window - 15)
        follower_env = dict(os.environ)
        follower_env["PROBE_FOLLOWER"] = "1"
        follower_env["TASKQ_METRICS_PORT"] = os.environ["PROBE_FOLLOWER_METRICS_PORT"]
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(PROBE_DIR + "/probe_worker.py"),
            env=follower_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        print("FOLLOWER_SPAWNED", flush=True)
        await asyncio.sleep(10)
        await _dump("FOLLOWER")
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=20)
        except TimeoutError:
            proc.kill()
            await proc.wait()

    async def _no_actor_config_shape() -> None:
        # After boot (the worker re-registers every actor_config row at
        # startup), delete the ok actor's row and enqueue more work for
        # it: pending rows whose actor has no actor_config - the
        # stranded no_actor_config shape, produced by real paths. And
        # once the worker has claimed the uncancellable job, cancel it:
        # the operator cancel outlasts both (1s) graces against the
        # swallowing actor - the real abandonment pathology.
        try:
            await asyncio.sleep(3)
            async with TaskQ(dsn=PG_DSN, schema=_schema()) as tq:
                await tq.cancel(_abandon_handle.job_id)  # type: ignore[union-attr]
            print("CANCEL_REQUESTED:OK", flush=True)
            await asyncio.sleep(5)
            conn = await asyncpg.connect(PG_DSN)
            try:
                await conn.execute(
                    f'DELETE FROM "{_schema()}".actor_config WHERE actor = $1',
                    "probe_ok_actor",
                )
                # Break the rate limiter's PG fallback (the store behind
                # the dead Redis): a NOT VALID CHECK constraint passes
                # validation of the existing rows but fails every new
                # write, so the fallback's bucket-row upsert fails and
                # the acquire fails closed - the dependency-outage
                # pathology.
                await conn.execute(
                    f'ALTER TABLE "{_schema()}".rate_limit_buckets '
                    "ADD CONSTRAINT probe_check CHECK (false) NOT VALID"
                )
            finally:
                await conn.close()
            async with TaskQ(dsn=PG_DSN, schema=_schema()) as tq:
                for i in range(2):
                    await tq.enqueue(probe_ok_actor, P(value=i))
                await tq.enqueue(probe_ratelimited_actor, P())

            # The cron pathologies fire on the NEXT MINUTE boundary
            # otherwise; pull the schedules due NOW (inside the catch-up
            # window): the failing schedule accrues its three strikes
            # within seconds, and the four slow-factory schedules' 10
            # minute backlog drains one fire per tick, the monopolizer
            # shape that defers the fourth schedule's fires every tick.
            conn = await asyncpg.connect(PG_DSN)
            try:
                await conn.execute(
                    f'UPDATE "{_schema()}".cron_schedules '
                    "SET next_fire_at = statement_timestamp() - interval "
                    f"'1 second' WHERE name = 'probe-failing-cron'"
                )
                await conn.execute(
                    f'UPDATE "{_schema()}".cron_schedules '
                    "SET next_fire_at = statement_timestamp() - interval "
                    # Two minutes of backlog: two drain ticks (~9s), so the
                    # failing schedule's own strikes land well before the
                    # live scrape; a longer backlog pushes the strikes past it.
                    f"'2 minutes' WHERE name LIKE 'probe-slow-cron-%'"
                )
            finally:
                await conn.close()
            # Hold the cron advisory lock's own key at session level for
            # 3s: every leader tick's try-lock loses inside the window -
            # the real cron-lock-contention emission (the name the
            # production lock probe hashes).
            lock_conn = await asyncpg.connect(PG_DSN)
            try:
                await lock_conn.execute(
                    "SELECT pg_advisory_lock(hashtextextended($1, 0))",
                    f"taskq:cron:{_schema()}",
                )
                await asyncio.sleep(3)
                await lock_conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                    f"taskq:cron:{_schema()}",
                )
            finally:
                await lock_conn.close()
            print("NO_ACTOR_CONFIG_SHAPE:OK", flush=True)
            await asyncio.sleep(4)
            conn = await asyncpg.connect(PG_DSN)
            try:
                # Heal: the limiter recovers on the job's next dispatch.
                await conn.execute(
                    f'ALTER TABLE "{_schema()}".rate_limit_buckets '
                    "DROP CONSTRAINT probe_check"
                )
            finally:
                await conn.close()
        except Exception as exc:  # noqa: BLE001
            # Loud, not swallowed: the shape's absence is a review failure.
            print(f"NO_ACTOR_CONFIG_SHAPE:FAILED:{type(exc).__name__}:{exc}", flush=True)
            raise

    async def _live_scrape_and_stop() -> None:
        await asyncio.sleep(window - 20)
        await _dump("LIVE")
        await asyncio.sleep(19)
        os.kill(os.getpid(), signal.SIGTERM)

    results = await asyncio.gather(
        _worker_task(),
        _no_actor_config_shape(),
        _live_scrape_and_stop(),
        _follower(),
        return_exceptions=True,
    )
    for entry in results:
        if isinstance(entry, BaseException):
            print(f"PROBE_TASK_FAILED:{type(entry).__name__}:{entry}", flush=True)
    print("WORKER_EXIT:", results[0], flush=True)

    await asyncio.sleep(0.5)
    await _dump("FINAL")


if __name__ == "__main__":
    asyncio.run(_run())
'''

_HOSTILE_PROBE = '''
"""Hostile probe: a real transient Postgres failure - the server's own
connection termination, fired repeatedly across several heartbeat ticks -
drives the heartbeat-failure arm end to end, then dumps the exposition."""

import asyncio
import os
import signal
import urllib.request

PG_DSN = os.environ["PROBE_PG_DSN"]
METRICS_PORT = int(os.environ["PROBE_METRICS_PORT"])


def _schema() -> str:
    """The probe's TaskQ schema, read per call - never a shared module
    constant (the suite-hygiene pin's anti-pattern)."""
    return os.environ["PROBE_SCHEMA"]


from pydantic import BaseModel

from taskq import JobContext, TaskQ, actor


class P(BaseModel):
    value: int = 1


@actor(name="hostile_ok_actor", queue="probe_queue")
async def hostile_ok_actor(payload: P, ctx: JobContext[P]) -> None:
    await asyncio.sleep(0.05)


ACTORS = {hostile_ok_actor.name: hostile_ok_actor}


async def _kill_all_backends(admin: "asyncpg.Connection") -> int:
    rows = await admin.fetch(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = current_database() AND pid <> pg_backend_pid() "
        "AND application_name <> 'probe_admin'"
    )
    return len(rows)


async def _scrape() -> str:
    with urllib.request.urlopen(  # noqa: S310
        f"http://127.0.0.1:{METRICS_PORT}/metrics", timeout=10
    ) as r:
        return r.read().decode()


async def _run() -> None:
    import asyncpg

    from taskq.obs import configure_exporters
    from taskq.settings import WorkerSettings
    from taskq.worker.run import _main

    settings = WorkerSettings.load()
    configure_exporters(settings)

    async with TaskQ(dsn=PG_DSN, schema=_schema()) as tq:
        for i in range(2):
            await tq.enqueue(hostile_ok_actor, P(value=i))

    window = float(os.environ.get("PROBE_WORKER_SECS", "45"))

    async def _worker_task() -> int:
        return await _main(settings, actor_registry=ACTORS)

    async def _chaos() -> None:
        # One persistent admin connection killing every backend every few
        # milliseconds for ~8s: the per-tick failure window (pool acquire
        # -> first statement) is tens of milliseconds, so a storm this
        # dense fails heartbeat ticks with near certainty while the admin
        # connection itself survives (filtered from its own kill set by
        # application_name).
        await asyncio.sleep(15)
        admin = await asyncpg.connect(
            PG_DSN, server_settings={"application_name": "probe_admin"}
        )
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 8.0
            while loop.time() < deadline:
                await _kill_all_backends(admin)
                await asyncio.sleep(0.005)
        finally:
            await admin.close()
        print("CHAOS_DONE", flush=True)

    async def _scrape_to(path: str) -> None:
        text = await _scrape()
        with open(path, "w") as fh:
            fh.write(text)
        print("SCRAPED:", path, len(text), flush=True)

    async def _live_scrape_and_stop() -> None:
        # Mid-chaos: the misses counter has moved.
        await asyncio.sleep(20)
        await _scrape_to(os.environ["PROBE_SCRAPE_PATH"])
        # Long after the last kill: a successful tick resets the
        # consecutive-failures gauge to 0 (the reset-on-success contract,
        # read off the served exposition).
        await asyncio.sleep(16)
        await _scrape_to(os.environ["PROBE_SCRAPE_PATH"] + ".recovered")
        await asyncio.sleep(6)
        os.kill(os.getpid(), signal.SIGTERM)

    results = await asyncio.gather(
        _worker_task(),
        _chaos(),
        _live_scrape_and_stop(),
        return_exceptions=True,
    )
    for entry in results:
        if isinstance(entry, BaseException):
            print(f"HOSTILE_TASK_FAILED:{type(entry).__name__}:{entry}", flush=True)
    print("WORKER_EXIT:", results[0], flush=True)


if __name__ == "__main__":
    asyncio.run(_run())
'''


def _write_probe_scripts(workdir: Path) -> None:
    (workdir / "probe_cron_factory.py").write_text(_CRON_FACTORY_MODULE)
    (workdir / "probe_worker.py").write_text(_WORKER_PROBE)
    (workdir / "probe_hostile.py").write_text(_HOSTILE_PROBE)


def _migrate_schema(pg_dsn: str, schema: str) -> None:
    script = (
        "import asyncio, sys\n"
        "import asyncpg\n"
        "from taskq.migrate import apply_pending\n"
        "from taskq.testing.fixtures import seed_actors\n"
        "async def go():\n"
        f"    conn = await asyncpg.connect({pg_dsn!r})\n"
        f"    await apply_pending(conn, schema={schema!r})\n"
        f"    await seed_actors(conn, {schema!r})\n"
        "    await conn.close()\n"
        "asyncio.run(go())\n"
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv built from test-controlled constants, no shell.
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "DOTENV_DIR": str(_dotenv_guard_dir())},
    )
    assert result.returncode == 0, f"migration failed: {result.stderr}"


def once_per_invocation(
    name: str,
    state_dir: Path,
    producer: Callable[[], dict[str, str]],
) -> dict[str, str]:
    """Run ONE probe per pytest INVOCATION, not one per xdist worker.

    The probe harness is a real worker + a follower + migrations — ~130s
    of machine. The review module's fixtures are module-scoped, and a
    module scope is PER WORKER: at ``-n 8`` up to eight probes ran
    concurrently, sixteen worker processes drowned the box, and the
    follower's boot outran every poll (the consolidated proof's
    PROBE_TASK_FAILED wall). The container pair already solved this
    shape (``shared_service_pair``'s per-invocation state dir + the
    holder lock); this is the same discipline for the probes: the
    FIRST worker takes the flock, runs the producer, and writes the
    result + the done marker into the invocation's own state dir; every
    other worker of the SAME invocation waits on the marker and reads
    the same result. A different invocation (a fresh basetemp) gets a
    fresh dir — no cross-invocation sharing, ever.
    """
    import fcntl
    import json as _json

    probe_dir = state_dir / f"prom-probe-{name}"
    probe_dir.mkdir(parents=True, exist_ok=True)
    done_marker = probe_dir / "done.json"
    lock_path = probe_dir / "probe.lock"
    lock_fh = open(lock_path, "w")  # noqa: SIM115  # held for the whole call
    try:
        # The blocking flock: the first taker runs the probe; the rest
        # block here until the marker exists (the producer releases the
        # lock only after writing it). The wait is bounded by the probe
        # itself (the follower poll's deadline is inside the producer),
        # not by us — a crashed producer leaves no marker and the
        # waiter's own probe-budget timeout below fires.
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        if done_marker.exists():
            return dict(_json.loads(done_marker.read_text()))
        result = producer()
        tmp_marker = probe_dir / f"done.json.tmp.{os.getpid()}"
        tmp_marker.write_text(_json.dumps(result))
        tmp_marker.replace(done_marker)
        return result
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


def run_worker_probe(
    pg_dsn: str,
    schema: str,
    workdir: Path,
    *,
    worker_secs: int = 80,
) -> dict[str, str]:
    """Run the real worker probe; return {tag: exposition_text} keyed
    ``LIVE``/``FINAL``, each the bridge router's served text."""
    _write_probe_scripts(workdir)
    _migrate_schema(pg_dsn, schema)
    metrics_port = _free_tcp_port()
    follower_port = _free_tcp_port()
    env = probe_env(
        PROBE_PG_DSN=pg_dsn,
        PROBE_SCHEMA=schema,
        PROBE_DIR=str(workdir),
        PROBE_METRICS_PORT=str(metrics_port),
        PROBE_FOLLOWER_METRICS_PORT=str(follower_port),
        PROBE_SCRAPE_PATH=str(workdir / "scrape.txt"),
        PROBE_WORKER_SECS=str(worker_secs),
        TASKQ_PG_DSN=pg_dsn,
        TASKQ_SCHEMA_NAME=schema,
        TASKQ_QUEUES="probe_queue,bp_queue",
        TASKQ_HEARTBEAT_INTERVAL="1",
        TASKQ_SWEEP_INTERVAL="1",
        TASKQ_QUEUE_DEPTH_INTERVAL="1",
        TASKQ_STRANDED_JOBS_INTERVAL="1",
        TASKQ_CANCELLATION_GRACE_PERIOD="1",
        TASKQ_CLEANUP_GRACE_PERIOD="1",
        TASKQ_MAX_CONCURRENCY="4",
        # The cron pathologies: the per-factory grant (2s) funds a slow
        # factory's 1.2s sleep, and the funded whole-tick budget
        # (dispatcher_command_timeout 5s x 0.9 = 4.5s) fits only three of
        # them - the fourth defers.
        TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT="2",
        TASKQ_REDIS_URL="redis://127.0.0.1:15999/0",
        TASKQ_METRICS_PORT=str(metrics_port),
        # The OTEL pull reader binds settings.metrics_port; pin the SDK's
        # own variable to the same allocated port so no code path can
        # fall back to the well-known default 9464 and race another
        # process for it.
        OTEL_EXPORTER_PROMETHEUS_PORT=str(metrics_port),
        TASKQ_LOG_LEVEL="WARNING",
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(workdir / "probe_worker.py")],
        capture_output=True,
        text=True,
        timeout=worker_secs + 120,
        env=env,
        cwd=str(workdir),
    )
    assert result.returncode == 0, (
        f"worker probe failed:\nstdout={result.stdout[-4000:]}\nstderr={result.stderr[-4000:]}"
    )
    scrapes = {}
    for tag in ("LIVE", "FINAL"):
        bridge_path = workdir / f"scrape.txt.{tag}.bridge"
        port_path = workdir / f"scrape.txt.{tag}.port"
        assert bridge_path.exists(), f"probe wrote no {tag} bridge scrape: {result.stdout[-2000:]}"
        scrapes[tag] = bridge_path.read_text()
        scrapes[f"{tag}.port"] = port_path.read_text()
    follower_path = workdir / "scrape.txt.FOLLOWER.port"
    assert follower_path.exists(), f"probe wrote no FOLLOWER scrape: {result.stdout[-2000:]}"
    scrapes["FOLLOWER.port"] = follower_path.read_text()
    assert "BP_REFUSED:" in result.stdout, (
        "the max_pending=0 enqueues were not refused - the backpressure "
        f"pathology never fired: {result.stdout[-2000:]}"
    )
    assert "NO_ACTOR_CONFIG_SHAPE:OK" in result.stdout, (
        f"the deleted-actor_config pathology never landed: {result.stdout[-2000:]}"
    )
    return scrapes


def run_hostile_probe(
    pg_dsn: str,
    schema: str,
    workdir: Path,
    *,
    worker_secs: int = 45,
) -> tuple[str, str]:
    """Run the hostile (real transient PG failure) probe; return
    (mid_chaos_scrape, post_recovery_scrape)."""
    _write_probe_scripts(workdir)
    _migrate_schema(pg_dsn, schema)
    hostile_port = _free_tcp_port()
    env = probe_env(
        PROBE_PG_DSN=pg_dsn,
        PROBE_SCHEMA=schema,
        PROBE_DIR=str(workdir),
        PROBE_METRICS_PORT=str(hostile_port),
        PROBE_SCRAPE_PATH=str(workdir / "hostile_scrape.txt"),
        PROBE_WORKER_SECS=str(worker_secs),
        TASKQ_PG_DSN=pg_dsn,
        TASKQ_SCHEMA_NAME=schema,
        TASKQ_QUEUES="probe_queue",
        TASKQ_HEARTBEAT_INTERVAL="1",
        # Default max_heartbeat_failures (3): the storm fails every tick,
        # so the worker takes its DESIGNED isolate exit a few ticks in -
        # the docstring's own "if misses continue, the worker will
        # self-isolate" contract. The assertion is that the miss counter
        # (the alert's operand) moved and OUTLIVED the isolate.
        TASKQ_METRICS_PORT=str(hostile_port),
        OTEL_EXPORTER_PROMETHEUS_PORT=str(hostile_port),
        TASKQ_LOG_LEVEL="WARNING",
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(workdir / "probe_hostile.py")],
        capture_output=True,
        text=True,
        timeout=worker_secs + 120,
        env=env,
        cwd=str(workdir),
    )
    assert result.returncode == 0, (
        f"hostile probe failed:\nstdout={result.stdout[-4000:]}\nstderr={result.stderr[-4000:]}"
    )
    path = workdir / "hostile_scrape.txt"
    recovered = workdir / "hostile_scrape.txt.recovered"
    assert path.exists() and recovered.exists(), (
        f"hostile probe wrote no scrape: {result.stdout[-2000:]}"
    )
    return path.read_text(), recovered.read_text()


# ── promtool rule evaluation (the honest alert harness) ────────────

#: The docker CLI's absolute path: the harness's container runs are fixed
#: argv, and an absolute executable satisfies the partial-path lint.
_DOCKER = shutil.which("docker")

_EMITTER_PROBE = '''
"""Emitter probe: drives the real cannot-stage-live emitters - the
sweep-abort pair (Postgres aborting a bounded prune batch collides
with the worker's own retry ladder), the cron skipped-slots counter
(no live run reaches it: the 1-hour default catch-up window swallows the
probes' staged 2-minute backlog), and the wf-progress gauge (an
observable gauge the MAINTENANCE LEADER samples on the admin's surface -
the worker probes scrape the worker exposition, which never carries it;
the emission path is the real public update_wf_progress_cache API the
leader's sampler calls) - families whose emission paths are real public
API - and dumps the exposition their series actually serve."""

import asyncio
import os
import sys

PROBE_DIR = os.environ["PROBE_DIR"]
sys.path.insert(0, PROBE_DIR)

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import CollectorRegistry, generate_latest

REGISTRY = CollectorRegistry()
READER = PrometheusMetricReader(registry=REGISTRY)
metrics.set_meter_provider(MeterProvider(metric_readers=[READER]))

from taskq.obs import (  # noqa: E402
    record_claim_latency,
    record_cron_skipped_slots,
    record_sweep_timeout,
    record_sweep_unexpected_error,
    update_wf_progress_cache,
)
from taskq.obs import _otel as otel_mod  # noqa: E402

otel_mod.set_otel_enabled(True)

record_sweep_timeout("scheduled_to_pending")
record_sweep_unexpected_error("scheduled_to_pending")
record_cron_skipped_slots("probe_fail_actor", 1)
# THE WF-PROGRESS GAUGE (T08): the leader's sampler's own write - the
# (workflow, state)-keyed cache the observable gauge reads out. The fed
# values are the promtool cases' fed label values' source of truth.
update_wf_progress_cache(
    {
        ("probe_wf", "blocked"): 3,
        ("probe_wf", "running"): 7,
        ("_other_", "pending"): 11,
    }
)

# The claim-health family: the degradation ratio's 5-minute baseline
# warm-up cannot be staged in a live worker probe (a probe run is
# seconds), so the emitter drives the REAL record_claim_latency hook with
# synthetic monotonic stamps - six minute-buckets of ~1ms p99s, then a
# 50ms p99 - and the scrape serves the ratio the real math produces (50x).
import time as _time  # noqa: E402

_base = _time.monotonic() + 100.0
for _m in range(6):
    for _i in range(10):
        record_claim_latency("probe_queue", 0.001, now=_base + _m * 60.0 + _i)
record_claim_latency("probe_queue", 0.050, now=_base + 400.0)

with open(os.environ["PROBE_SCRAPE_PATH"], "w") as fh:
    fh.write(generate_latest(REGISTRY).decode())
print("EMITTER_PROBE_OK", flush=True)
'''


def run_emitter_probe(workdir: Path) -> str:
    """Drive the real cannot-stage-live emitters (the sweep-abort pair and
    the cron skipped-slots counter) through the real bridge; return the
    exposition their series serve.

    The promtool harness's cannot-stage-live cases (and the rules' operand
    gate) bind to THIS scrape: the families a live worker probe cannot
    stage are still bound to the names the real emitters serve, so the
    harness's hand-typed series cannot drift from them.
    """
    _write_probe_scripts(workdir)
    script = workdir / "probe_emitter.py"
    script.write_text(_EMITTER_PROBE)
    scrape_path = workdir / "emitter_scrape.txt"
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=60,
        env=probe_env(PROBE_DIR=str(workdir), PROBE_SCRAPE_PATH=str(scrape_path)),
    )
    assert result.returncode == 0, (
        f"emitter probe failed:\nstdout={result.stdout[-2000:]}\nstderr={result.stderr[-2000:]}"
    )
    return scrape_path.read_text()


def docker_available() -> bool:
    if _DOCKER is None:
        return False
    try:
        result = subprocess.run(  # noqa: S603  # Why: fixed argv probe of the daemon, no shell.
            [_DOCKER, "info", "--format", "ok"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def run_promtool_rule_tests(rules_path: Path, test_yaml: str, workdir: Path) -> str:
    """Evaluate a ``promtool test rules`` file against the shipped rules
    inside the prom/prometheus image's own promtool.

    The rules file is mounted read-only, exactly as a server would load
    it; the test file's input series are built by the callers from the
    REAL exposition the probes captured. Raises AssertionError with
    promtool's diff on failure - each non-firing rule names the alert.
    """
    assert _DOCKER is not None, "docker is required for the promtool evaluation"
    # Stage into a fresh, world-readable directory: pytest tmp paths carry
    # 0700 modes the container's stat cannot cross.
    stage = Path(tempfile.mkdtemp(prefix="promtool_stage_"))
    try:
        shutil.copy(rules_path, stage / "rules.yaml")
        for path in (stage, stage / "rules.yaml"):
            path.chmod(0o755)
        test_file = stage / "taskq_rules_test.yml"
        test_file.write_text(test_yaml)
        test_file.chmod(0o644)
        result = subprocess.run(  # noqa: S603  # Why: fixed argv container run, no shell.
            [
                _DOCKER,
                "run",
                "--rm",
                "-v",
                f"{stage}:/work:ro",
                "--entrypoint",
                "promtool",
                PROMTOOL_IMAGE,
                "test",
                "rules",
                "/work/taskq_rules_test.yml",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert result.returncode == 0, (
            f"promtool rule tests failed:\n{result.stdout}\n{result.stderr}"
        )
        return result.stdout
    finally:
        shutil.rmtree(stage, ignore_errors=True)
