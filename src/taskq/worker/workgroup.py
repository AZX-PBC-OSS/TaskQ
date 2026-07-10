"""Workgroup supervisor — spawns and manages multiple TaskQ worker processes.

A workgroup is a single-process orchestrator that manages N child ``taskq worker``
subprocesses, each with potentially different queues, poll intervals, concurrency
caps, etc.  The supervisor restarts children that crash, optionally health-checks
them via the database, and cleanly propagates shutdown signals.

Config format (TOML)::

    actors = "myapp.actors:registry"

    [defaults]
    poll_interval = 1.0
    max_concurrency = 4

    [supervisor]
    shutdown_grace = 30.0
    backoff_initial = 0.5
    backoff_max = 30.0
    backoff_factor = 2.0
    burst_limit = 10
    burst_window = 60.0

    [[workers]]
    name = "api"
    queues = ["default"]
    max_concurrency = 8
    poll_interval = 0.5

    [workers.health]
    enabled = true
    check_interval = 15
    stale_after = 60
    startup_grace = 15.0
    consecutive_failure_limit = 3
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq.constants import (
    _IDENT_RE as _SCHEMA_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining; same pattern as run.py.
)
from taskq.obs import get_logger

__all__ = [
    "WorkerSpec",
    "WorkgroupConfig",
    "load_workgroup_config",
    "run_forever",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


# ── Config model ──────────────────────────────────────────────────────────


@dataclass(slots=True)
class SupervisorConfig:
    """Global supervisor behaviour tunables."""

    shutdown_grace: float = 30.0  # seconds to wait for children during shutdown
    backoff_initial: float = 0.5  # first restart delay (seconds)
    backoff_max: float = 30.0  # ceiling on restart delay
    backoff_factor: float = 2.0  # multiplier per successive crash
    burst_limit: int = 10  # max restarts within burst_window before giving up
    burst_window: float = 60.0  # rolling window for burst counting (seconds)
    health_pg_dsn: str | None = (
        None  # override PG DSN for health checks (falls back to TASKQ_PG_DSN_DIRECT)
    )
    health_pg_schema: str | None = (
        None  # override PG schema for health checks (falls back to TASKQ_SCHEMA_NAME)
    )


@dataclass(slots=True)
class WorkerHealthConfig:
    """Per-worker health-check configuration."""

    enabled: bool = False
    check_interval: float = 15.0  # seconds between DB checks
    stale_after: float = 60.0  # seconds before a worker is considered hung
    startup_grace: float = 15.0  # grace period after spawn before first health check
    consecutive_failure_limit: int = 3  # consecutive DB query failures before declaring dead


@dataclass(slots=True)
class WorkerSpec:
    """Configuration for a single worker process managed by the workgroup."""

    name: str
    queues: list[str]
    poll_interval: float = 1.0
    max_concurrency: int = 8
    worker_group: str = "default"
    force_update_actor_config: bool = False
    health: WorkerHealthConfig = field(default_factory=WorkerHealthConfig)

    def cli_args(self) -> list[str]:
        """Build the CLI argument list for this worker."""
        args: list[str] = []
        for q in self.queues:
            args.extend(["--queues", q])
        args.extend(["--poll-interval", str(self.poll_interval)])
        args.extend(["--max-concurrency", str(self.max_concurrency)])
        args.extend(["--worker-group", self.worker_group])
        if self.force_update_actor_config:
            args.append("--force-update-actor-config")
        return args


@dataclass(slots=True)
class WorkgroupConfig:
    """Top-level workgroup configuration loaded from TOML."""

    actors: str
    supervisor: SupervisorConfig = field(default_factory=SupervisorConfig)
    workers: list[WorkerSpec] = field(default_factory=lambda: cast(list[WorkerSpec], []))  # type: ignore[arg-type]  # Why: dataclass field default_factory with cast; pyright cannot verify the cast result type matches the dataclass field type.
    defaults: dict[str, Any] = field(default_factory=dict)  # type: ignore[arg-type]  # Why: dataclass field default_factory returns dict[str, Any]; pyright reports Any as incompatible with the field's generic type.

    @classmethod
    def from_toml(cls, path: Path) -> WorkgroupConfig:
        """Load and validate the workgroup TOML configuration."""
        raw: dict[str, Any] = tomllib.loads(path.read_text())

        actors: Any = raw.get("actors")
        if not actors or not isinstance(actors, str):
            raise ValueError("workgroup config must define 'actors' (str)")
        if ":" not in actors:
            raise ValueError(f"actors must be module:attr syntax, got {actors!r}")

        defaults: dict[str, Any] = dict(raw.get("defaults", {}))

        sup_raw: dict[str, Any] = raw.get("supervisor", {})
        supervisor = SupervisorConfig(
            shutdown_grace=float(sup_raw.get("shutdown_grace", 30.0)),
            backoff_initial=float(sup_raw.get("backoff_initial", 0.5)),
            backoff_max=float(sup_raw.get("backoff_max", 30.0)),
            backoff_factor=float(sup_raw.get("backoff_factor", 2.0)),
            burst_limit=int(sup_raw.get("burst_limit", 10)),
            burst_window=float(sup_raw.get("burst_window", 60.0)),
            health_pg_dsn=sup_raw.get("health_pg_dsn"),
            health_pg_schema=sup_raw.get("health_pg_schema"),
        )

        raw_workers: list[dict[str, Any]] = raw.get("workers", [])  # type: ignore[assignment]  # Why: tomllib returns Any; validated below.
        if not raw_workers:
            raise ValueError("workgroup config must define at least one [[workers]] entry")

        names: set[str] = set()
        workers: list[WorkerSpec] = []
        for i, w in enumerate(raw_workers):
            name: Any = w.get("name")
            if not name or not isinstance(name, str):
                raise ValueError(f"workers[{i}] must have a 'name' (str)")
            if name in names:
                raise ValueError(f"duplicate worker name: {name!r}")
            names.add(name)

            health_raw: dict[str, Any] = w.get("health", {})
            health = WorkerHealthConfig(
                enabled=bool(health_raw.get("enabled", False)),
                check_interval=float(health_raw.get("check_interval", 15.0)),
                stale_after=float(health_raw.get("stale_after", 60.0)),
                startup_grace=float(health_raw.get("startup_grace", 15.0)),
                consecutive_failure_limit=int(health_raw.get("consecutive_failure_limit", 3)),
            )

            workers.append(
                WorkerSpec(
                    name=name,
                    queues=_require_list_str(w, "queues", defaults.get("queues", ["default"])),
                    poll_interval=float(w.get("poll_interval", defaults.get("poll_interval", 1.0))),
                    max_concurrency=int(
                        w.get("max_concurrency", defaults.get("max_concurrency", 8))
                    ),
                    worker_group=str(
                        w.get("worker_group", defaults.get("worker_group", "default"))
                    ),
                    force_update_actor_config=bool(
                        w.get(
                            "force_update_actor_config",
                            defaults.get("force_update_actor_config", False),
                        )
                    ),
                    health=health,
                )
            )

        cfg = cls(actors=actors, supervisor=supervisor, workers=workers, defaults=defaults)
        _validate_config(cfg)
        return cfg


def _require_list_str(cfg: dict[str, Any], key: str, fallback: list[str]) -> list[str]:
    """Extract a list[str] from config or fallback; validate types."""
    val: Any = cfg.get(key, fallback)
    if not isinstance(val, list) or not all(isinstance(v, str) for v in val):  # type: ignore[arg-type]  # Why: tomllib returns Any; list check above ensures val is iterable.
        raise ValueError(f"{key!r} must be a list of strings, got {val!r}")
    return val  # type: ignore[return-value]  # Why: val is narrowed to list[str] by the isinstance checks above, but pyright cannot propagate the element-type narrowing through all().


def load_workgroup_config(path: Path) -> WorkgroupConfig:
    """Load a workgroup configuration from a TOML file."""
    return WorkgroupConfig.from_toml(path)


def _validate_config(cfg: WorkgroupConfig) -> None:
    """Validate numeric domains and invariants; raise ValueError on misconfiguration."""
    scfg = cfg.supervisor

    if scfg.shutdown_grace <= 0:
        raise ValueError(f"supervisor.shutdown_grace must be > 0, got {scfg.shutdown_grace}")
    if scfg.backoff_initial <= 0:
        raise ValueError(f"supervisor.backoff_initial must be > 0, got {scfg.backoff_initial}")
    if scfg.backoff_max < scfg.backoff_initial:
        raise ValueError(
            f"supervisor.backoff_max ({scfg.backoff_max}) must be >= "
            f"backoff_initial ({scfg.backoff_initial})"
        )
    if scfg.backoff_factor < 1.0:
        raise ValueError(f"supervisor.backoff_factor must be >= 1.0, got {scfg.backoff_factor}")
    if scfg.burst_limit <= 0:
        raise ValueError(f"supervisor.burst_limit must be > 0, got {scfg.burst_limit}")
    if scfg.burst_window <= 0:
        raise ValueError(f"supervisor.burst_window must be > 0, got {scfg.burst_window}")
    if scfg.health_pg_schema is not None and not _SCHEMA_RE.match(scfg.health_pg_schema):
        raise ValueError(
            f"supervisor.health_pg_schema {scfg.health_pg_schema!r} is not a valid "
            f"schema identifier (must match {_SCHEMA_RE.pattern})"
        )

    for w in cfg.workers:
        if len(w.name) > 64:
            raise ValueError(f"worker[{w.name!r}].name must be <= 64 chars (socket path limit)")
        if w.poll_interval <= 0:
            raise ValueError(f"worker[{w.name!r}].poll_interval must be > 0, got {w.poll_interval}")
        if w.max_concurrency <= 0:
            raise ValueError(
                f"worker[{w.name!r}].max_concurrency must be > 0, got {w.max_concurrency}"
            )
        if w.health.enabled:
            if w.health.check_interval <= 0:
                raise ValueError(
                    f"worker[{w.name!r}].health.check_interval must be > 0, "
                    f"got {w.health.check_interval}"
                )
            if w.health.stale_after <= 0:
                raise ValueError(
                    f"worker[{w.name!r}].health.stale_after must be > 0, got {w.health.stale_after}"
                )
            if w.health.check_interval >= w.health.stale_after:
                raise ValueError(
                    f"worker[{w.name!r}].health.check_interval "
                    f"({w.health.check_interval}) must be < stale_after "
                    f"({w.health.stale_after})"
                )
            if w.health.startup_grace < 0:
                raise ValueError(
                    f"worker[{w.name!r}].health.startup_grace must be >= 0, "
                    f"got {w.health.startup_grace}"
                )
            if w.health.consecutive_failure_limit <= 0:
                raise ValueError(
                    f"worker[{w.name!r}].health.consecutive_failure_limit must be > 0, "
                    f"got {w.health.consecutive_failure_limit}"
                )


# ── Supervisor runtime ────────────────────────────────────────────────────

_STREAM_LIMIT: int = 1 << 20  # 1 MiB buffer for subprocess output lines


@dataclass
class _ChildState:
    """Runtime state for one managed child process."""

    spec: WorkerSpec
    process: asyncio.subprocess.Process | None = None
    restart_count: int = 0
    restart_times: list[float] = field(default_factory=lambda: cast(list[float], []))  # type: ignore[arg-type]  # Why: dataclass field default_factory with cast; pyright cannot verify the cast result type matches the dataclass field type.
    instance_id: UUID = field(default_factory=new_uuid)
    backoff: float = 0.0
    restart_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    spawned_at: float = 0.0
    health_failures: int = 0
    stdout_task: asyncio.Task[None] | None = None
    stderr_task: asyncio.Task[None] | None = None


def _health_check_sql(schema: str) -> str:
    """Build the health-check query for a worker.

    The *schema* parameter is validated against _IDENT_RE inline (defence-in-depth
    even though WorkerSettings already constrains it via the regex Field).
    """
    if not _SCHEMA_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        f'SELECT pid, last_seen_at FROM "{schema}".workers '  # noqa: S608  # Why: schema validated against _IDENT_RE immediately above.
        "WHERE workgroup_instance = $1 AND worker_label = $2 "
        "ORDER BY last_seen_at DESC LIMIT 1"
    )


async def _child_health_check(
    child: _ChildState,
    pg_pool: asyncpg.Pool,
    schema: str,
    cfg: WorkerHealthConfig,
    wg_instance: UUID,
) -> bool:
    """Return True if the child appears healthy (recent DB heartbeat).

    Errors on the healthy side for transient DB blips, but after
    ``consecutive_failure_limit`` consecutive query failures the check returns
    False to prevent a persistent DB outage from masking hung workers.
    """
    sql = _health_check_sql(schema)
    try:
        async with pg_pool.acquire(timeout=2.0) as conn:
            row = await conn.fetchrow(sql, wg_instance, child.spec.name)
    except Exception as exc:
        child.health_failures += 1
        logger.warning(
            "workgroup.health_query_failed",
            worker=child.spec.name,
            error=str(exc),
            consecutive_failures=child.health_failures,
        )
        return child.health_failures < cfg.consecutive_failure_limit

    child.health_failures = 0

    if row is None:
        logger.warning(
            "workgroup.health_row_missing",
            worker=child.spec.name,
            instance_id=str(child.instance_id),
        )
        return False

    pid: int = row["pid"]
    last_seen = row["last_seen_at"]
    if pid != (child.process.pid if child.process else None):
        logger.debug(
            "workgroup.health_pid_mismatch",
            worker=child.spec.name,
            db_pid=pid,
            local_pid=child.process.pid if child.process else None,
        )
        return False

    if last_seen is None:
        return False

    try:
        age = time.time() - last_seen.timestamp()
    except (AttributeError, OSError, OverflowError):
        logger.warning(
            "workgroup.health_bad_timestamp",
            worker=child.spec.name,
            last_seen_type=type(last_seen).__name__,
        )
        return False
    if age > cfg.stale_after:
        logger.warning(
            "workgroup.health_stale",
            worker=child.spec.name,
            age_seconds=round(age, 1),
            stale_after=cfg.stale_after,
        )
        return False

    return True


async def _kill_child(child: _ChildState) -> None:
    """Force-kill a child (SIGTERM + 5 s grace, then SIGKILL)."""
    proc = child.process
    if proc is None or proc.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
                await proc.wait()


async def _spawn_child(
    child: _ChildState,
    actors: str,
    wg_instance: UUID,
) -> None:
    """Spawn a child worker subprocess.

    Sets ``child.process``, ``child.instance_id``, ``child.spawned_at``,
    ``child.stdout_task``, and ``child.stderr_task``.  The caller owns the
    task lifecycle (cancellation, awaiting) via those fields.
    """
    child.instance_id = new_uuid()
    child.spawned_at = time.monotonic()
    child.health_failures = 0
    health_path = f"/tmp/taskq_health_{child.spec.name}_{child.instance_id}.sock"  # noqa: S108  # Why: temp socket path for workgroup children; passed via --health-socket-path CLI arg.

    cmd = [
        sys.executable,
        "-m",
        "taskq",
        "worker",
        "--actors",
        actors,
        "--worker-label",
        child.spec.name,
        "--workgroup-instance",
        str(wg_instance),
        "--health-socket-path",
        health_path,
        *child.spec.cli_args(),
    ]

    logger.info(
        "workgroup.spawn",
        worker=child.spec.name,
        instance_id=str(child.instance_id),
        health_socket=health_path,
        queues=child.spec.queues,
        poll_interval=child.spec.poll_interval,
        max_concurrency=child.spec.max_concurrency,
    )

    child.process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=_STREAM_LIMIT,
    )

    child.stdout_task = asyncio.create_task(
        _stream_output(child.process.stdout, child.spec.name, "info")
    )
    child.stderr_task = asyncio.create_task(
        _stream_output(child.process.stderr, child.spec.name, "warning")
    )


async def _stream_output(
    stream: asyncio.StreamReader | None,
    name: str,
    level: str,
) -> None:
    """Forward child process output lines to the supervisor logger."""
    if stream is None:
        return
    log_fn: Any = getattr(logger, level)
    while True:
        line = await stream.readline()
        if not line:
            break
        log_fn("workgroup.child_output", worker=name, line=line.decode(errors="replace").rstrip())


def _prune_burst(child: _ChildState, cfg: SupervisorConfig) -> bool:
    """Prune expired restart-times and check the burst limit.

    Returns True if restarts are still allowed.  Resets backoff to initial
    when the burst window clears after a stable period.
    """
    now = time.monotonic()
    before = len(child.restart_times)
    child.restart_times = [t for t in child.restart_times if now - t < cfg.burst_window]
    if before > 0 and len(child.restart_times) == 0:
        child.backoff = cfg.backoff_initial
        child.restart_count = 0
    child.restart_times.append(now)
    return len(child.restart_times) <= cfg.burst_limit


async def _handle_child_exit(
    child: _ChildState,
    actors: str,
    wg_instance: UUID,
    scfg: SupervisorConfig,
    shutting_down: asyncio.Event,
) -> float | None:
    """React to a child process exiting; compute restart delay.

    Must be called with ``child.restart_lock`` held.  Does **not** sleep or
    spawn — callers must release the lock before sleeping.

    Returns:
        Backoff delay in seconds if a restart should be attempted after
        sleeping, or ``None`` if no restart is needed (burst limit
        exceeded, shutting down, or process already gone).
    """
    proc = child.process
    if proc is None or proc.returncode is None:
        return None

    rc = proc.returncode
    logger.info("workgroup.child_exit", worker=child.spec.name, exit_code=rc)

    child.process = None

    if shutting_down.is_set():
        return None

    if not _prune_burst(child, scfg):
        logger.critical(
            "workgroup.burst_limit_exceeded",
            worker=child.spec.name,
            restarts=len(child.restart_times),
            window_s=scfg.burst_window,
        )
        return None

    delay = min(child.backoff, scfg.backoff_max)
    child.backoff = min(child.backoff * scfg.backoff_factor, scfg.backoff_max)
    child.restart_count += 1
    logger.info(
        "workgroup.restart_scheduled",
        worker=child.spec.name,
        delay_s=round(delay, 1),
        attempt=child.restart_count,
    )
    return delay


async def run_forever(config_path: Path) -> None:
    """Load config, spawn children, manage lifecycle until a signal arrives.

    Blocks until SIGTERM or SIGINT, then shuts down all children gracefully.
    Returns normally after shutdown — the CLI caller handles the exit code.
    """
    config = load_workgroup_config(config_path)
    scfg = config.supervisor
    wg_instance = new_uuid()

    logger.info(
        "workgroup.start",
        actors=config.actors,
        instance_id=str(wg_instance),
        worker_count=len(config.workers),
    )

    # ── Resolve health-check PG pool (only when needed) ───────────────
    from taskq.settings import WorkerSettings

    pg_pool: asyncpg.Pool | None = None
    pg_schema: str = "taskq"

    health_workers = [w for w in config.workers if w.health.enabled]
    if health_workers:
        try:
            if scfg.health_pg_dsn:
                pg_dsn = scfg.health_pg_dsn
                pg_schema = scfg.health_pg_schema or "taskq"
            else:
                settings = WorkerSettings.load()
                pg_schema = settings.schema_name
                pg_dsn = str(settings.resolved_pg_dsn_direct)
            pg_pool = await asyncpg.create_pool(
                pg_dsn,
                min_size=1,
                max_size=len(health_workers) + 1,
            )
            logger.info(
                "workgroup.health_pool_ready",
                schema=pg_schema,
                workers=[w.name for w in health_workers],
            )
        except Exception as exc:
            logger.critical("workgroup.health_pool_failed", error=str(exc))
            sys.exit(1)

    # ── Build child state machines ────────────────────────────────────
    children: dict[str, _ChildState] = {}
    for spec in config.workers:
        state = _ChildState(spec=spec)
        state.backoff = scfg.backoff_initial
        children[spec.name] = state

    shutting_down = asyncio.Event()

    # ── Emit warnings for risky config ────────────────────────────────
    for w in config.workers:
        if w.force_update_actor_config:
            logger.warning(
                "workgroup.force_update_actor_config_enabled",
                worker=w.name,
                note="Permanent force-update will silently overwrite actor_config on "
                "every restart. Set to false after the first deploy with config changes.",
            )

    # ── Spawn all children initially ──────────────────────────────────
    for child in children.values():
        try:
            await _spawn_child(child, config.actors, wg_instance)
        except Exception:
            logger.exception(
                "workgroup.spawn_failed",
                worker=child.spec.name,
            )
            # Continue — liveness_monitor will retry at next tick.

    # ── Signal handler — just sets the event; real cleanup follows ────
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        if not shutting_down.is_set():
            logger.info("workgroup.shutdown_signal")
            shutting_down.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    # ── Liveness monitor — detects exited children and restarts them ──
    async def liveness_monitor() -> None:
        while not shutting_down.is_set():
            for child in list(children.values()):
                proc = child.process
                if proc is not None and proc.returncode is not None:
                    async with child.restart_lock:
                        # Cancel stale stream tasks from the dead process.
                        for t in (child.stdout_task, child.stderr_task):
                            if t is not None and not t.done():
                                t.cancel()
                        child.stdout_task = None
                        child.stderr_task = None
                        delay = await _handle_child_exit(
                            child, config.actors, wg_instance, scfg, shutting_down
                        )
                    # Lock released.  Sleep outside the lock, racing against shutdown.
                    if delay is not None:
                        _sleep_task = asyncio.create_task(asyncio.sleep(delay))
                        _shutdown_wait_task = asyncio.create_task(shutting_down.wait())
                        _done, _pending = await asyncio.wait(
                            [_sleep_task, _shutdown_wait_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for _t in _pending:
                            _t.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await _t
                        # Re-acquire briefly to spawn the replacement.
                        if not shutting_down.is_set():
                            async with child.restart_lock:
                                if child.process is None:
                                    try:
                                        await _spawn_child(child, config.actors, wg_instance)
                                    except Exception:
                                        logger.exception(
                                            "workgroup.spawn_failed",
                                            worker=child.spec.name,
                                        )
            await asyncio.sleep(0.5)

    # ── Health-check loop — kills hung workers via DB ─────────────────
    async def health_loop() -> None:
        if pg_pool is None:
            return
        while not shutting_down.is_set():
            for child in list(children.values()):
                if not child.spec.health.enabled:
                    continue
                if (
                    child.spawned_at > 0
                    and (time.monotonic() - child.spawned_at) < child.spec.health.startup_grace
                ):
                    continue
                async with child.restart_lock:
                    proc = child.process
                    if proc is None or proc.returncode is not None:
                        continue
                    healthy = await _child_health_check(
                        child, pg_pool, pg_schema, child.spec.health, wg_instance
                    )
                    if not healthy:
                        logger.warning("workgroup.health_kill", worker=child.spec.name)
                        await _kill_child(child)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    shutting_down.wait(),
                    timeout=min(
                        (
                            w.spec.health.check_interval
                            for w in children.values()
                            if w.spec.health.enabled
                        ),
                        default=15.0,
                    ),
                )

    # ── Run foreground + background loops in a TaskGroup ──────────────
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(liveness_monitor())
            tg.create_task(health_loop())
            await shutting_down.wait()
    except* Exception as eg:
        for exc in eg.exceptions:
            logger.error("workgroup.background_task_failed", error=str(exc))
        # Reset child state to a safe baseline before shutdown.
        for child in children.values():
            async with child.restart_lock:
                if child.process is not None and child.process.returncode is not None:
                    child.process = None

    # ── Graceful shutdown ─────────────────────────────────────────────
    logger.info("workgroup.shutdown_begin", grace_s=scfg.shutdown_grace)

    # Forward SIGTERM to all living children (under lock).
    for child in children.values():
        async with child.restart_lock:
            proc = child.process
            if proc is not None and proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.send_signal(signal.SIGTERM)

    # Wait concurrently for all children.
    wait_tasks: list[asyncio.Task[int]] = []
    for child in children.values():
        async with child.restart_lock:
            proc = child.process
            if proc is not None and proc.returncode is None:
                wait_tasks.append(asyncio.create_task(proc.wait()))
    if wait_tasks:
        _done, pending = await asyncio.wait(wait_tasks, timeout=scfg.shutdown_grace)
        for task in pending:
            task.cancel()
        for task in wait_tasks:
            if not task.done():
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    # Force-kill any survivors.
    for child in children.values():
        async with child.restart_lock:
            proc = child.process
            if proc is not None and proc.returncode is None:
                logger.warning(
                    "workgroup.child_shutdown_force_kill",
                    worker=child.spec.name,
                )
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()

    # Cancel stream tasks.
    for child in children.values():
        for t in (child.stdout_task, child.stderr_task):
            if t is not None and not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t

    if pg_pool:
        await pg_pool.close()

    logger.info("workgroup.shutdown_complete")
