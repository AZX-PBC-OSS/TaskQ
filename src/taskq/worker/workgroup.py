"""Workgroup supervisor, spawns and manages multiple TaskQ worker processes.

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
    shutdown_grace = 100.0
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
    pg_credential_provider = "infra.identity:pg_credentials"

    [workers.health]
    enabled = true
    check_interval = 15
    stale_after = 60
    startup_grace = 15.0
    consecutive_failure_limit = 3

A ``pg_credential_provider`` set at the ``[defaults]`` level applies to
every worker, and TOML has no per-worker ``null``, a worker cannot opt
out of a defaults-level provider, so a workgroup mixing provider-backed
and provider-less workers must set the field per worker instead of in
``[defaults]``.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import signal
import sys
import time
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeGuard, cast
from uuid import UUID

import asyncpg
import structlog

from taskq._close import CLOSE_TIMEOUT_SECS, close_pool_bounded
from taskq._ids import new_uuid
from taskq.connections import statement_cache_kwargs
from taskq.constants import (
    _IDENT_RE as _SCHEMA_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining; same pattern as run.py.
)
from taskq.obs import get_logger

if TYPE_CHECKING:
    from taskq.settings import WorkerSettings

__all__ = [
    "DEFAULT_SHUTDOWN_GRACE_SECS",
    "WorkerSpec",
    "WorkgroupConfig",
    "load_workgroup_config",
    "run_forever",
    "worst_case_shutdown_seconds",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_STREAM_LIMIT: int = 1 << 20
"""Default per-line buffer for a child's stdout/stderr (1 MiB).

Per-worker override: ``WorkerSpec.stream_limit``. A line longer than this
is truncated and reported (see :func:`_read_line`), never fatal.
"""

_HEALTH_QUERY_TIMEOUT_SECS: float = 2.0
"""Client-side deadline on the health-check query itself.

Why a bound: the pool acquire beside it is already bounded (2.0 s), but
the query was not, a server that accepts the query and never answers
parked the health loop inside the child's ``restart_lock``, and because
the liveness monitor and the shutdown path acquire the same locks
sequentially, one black-holed query froze restart scheduling for every
child and wedged the supervisor's SIGTERM forwarding, bounded only by
TCP keepalives (minutes).

Why client-side rather than a session ``statement_timeout``: the batch
paths bind theirs with ``SET LOCAL`` inside a transaction
(taskq.backend._sweeps), a scope an autocommit pool check does not
have, and capture/restore on a pooled session adds round trips that can
themselves hang. Why 2.0: consistent with the neighboring pool-acquire
bound, the query is an indexed LIMIT-1 lookup, so 2.0 s is already
generous. A timeout is a client-side deadline, transient per
``taskq.worker._transient``, so it lands in the existing
``consecutive_failure_limit`` accounting, which errs on the healthy
side for exactly the DB-outage case where killing children would be
wrong.
"""


# ── Child health socket path ──────────────────────────────────────────────


def _health_socket_path(name: str, instance_id: UUID) -> str:
    """The child worker's mandatory health socket path.

    The single construction of the path: both the spawn command line and
    the config validator's worker-name budget derive from it, so the
    budget and the built path cannot drift apart.
    """
    return f"/tmp/taskq_health_{name}_{instance_id}.sock"  # noqa: S108  # Why: workgroup-child socket files live in /tmp, the shortest prefix that keeps the full path inside every platform's AF_UNIX budget; handed to the child via --health-socket-path.


_SUN_PATH_BUDGET: int = 104 - 1
"""Usable ``sockaddr_un.sun_path`` chars on the tightest supported
platform (macOS/BSD: 104 bytes including the NUL terminator). Budgeted
for the tightest platform rather than the local one, a config authored
on a Linux host (107 usable chars) must still bind on a macOS deploy."""


_MAX_WORKER_NAME_LEN: int = _SUN_PATH_BUDGET - len(_health_socket_path("", UUID(int=0)))
"""Longest worker name whose health socket path still binds on every
supported platform. With the name empty the path is exactly its fixed
overhead, prefix, separator, instance-id uuid, suffix, so the name
budget is the platform budget minus that overhead, derived from the real
construction rather than restated beside it."""


# ── Config model ──────────────────────────────────────────────────────────

DEFAULT_SHUTDOWN_GRACE_SECS: float = 100.0
"""Default for ``[supervisor].shutdown_grace``, the supervisor's
SIGTERM-to-SIGKILL window for its children.

Why 100.0: the window must cover a child's modelled worst-case clean
exit, ``WorkerSettings.worst_case_shutdown_seconds`` = cancellation
grace (30s) + cleanup grace (10s) + the bounded-close teardown tail
(8 sequential closes x 5s + 2s publish drain = 42s) = 82s, plus ~22%
margin for loop jitter and the shutdown watchdog's non-instantaneous
deadline trip (the dump interval and bounded metrics flush land inside
the tail, but the tail is a model, not a bound the supervisor enforces).
The old 30.0 default sat below even the children's 40s release floor
(cancellation + cleanup graces): the supervisor SIGKILLed children
before a held release could land, and every interrupted job rode the
lease-expiry crash path instead. Operators who want the tighter,
release-floor-only window can still set it explicitly; the startup
warning names the numbers rather than refusing.
"""


@dataclass(slots=True)
class SupervisorConfig:
    """Global supervisor behaviour tunables."""

    shutdown_grace: float = (
        DEFAULT_SHUTDOWN_GRACE_SECS  # seconds to wait for children during shutdown
    )
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
    """Per-worker health-check configuration.

    The freshness verdict is server-side and exclusive at the boundary:
    a worker whose server-measured age equals ``stale_after`` exactly is
    already stale (the predicate is ``last_seen_at > now() - stale_after``,
    strict ``>``).
    """

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
    pg_credential_provider: str | None = None  # module:attr, forwarded to the child CLI
    stream_limit: int = _STREAM_LIMIT  # per-line stdout/stderr buffer (bytes)
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
        if self.pg_credential_provider is not None:
            args.extend(["--pg-credential-provider", self.pg_credential_provider])
        return args


@dataclass(slots=True)
class WorkgroupConfig:
    """Top-level workgroup configuration loaded from TOML."""

    actors: str
    supervisor: SupervisorConfig = field(default_factory=SupervisorConfig)
    workers: list[WorkerSpec] = field(default_factory=list[WorkerSpec])
    defaults: dict[str, Any] = field(default_factory=dict[str, Any])

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
            shutdown_grace=float(sup_raw.get("shutdown_grace", DEFAULT_SHUTDOWN_GRACE_SECS)),
            backoff_initial=float(sup_raw.get("backoff_initial", 0.5)),
            backoff_max=float(sup_raw.get("backoff_max", 30.0)),
            backoff_factor=float(sup_raw.get("backoff_factor", 2.0)),
            burst_limit=int(sup_raw.get("burst_limit", 10)),
            burst_window=float(sup_raw.get("burst_window", 60.0)),
            health_pg_dsn=sup_raw.get("health_pg_dsn"),
            health_pg_schema=sup_raw.get("health_pg_schema"),
        )

        raw_workers: list[dict[str, Any]] = raw.get("workers", [])
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
                    pg_credential_provider=_optional_str(
                        w, "pg_credential_provider", defaults.get("pg_credential_provider")
                    ),
                    stream_limit=int(
                        w.get("stream_limit", defaults.get("stream_limit", _STREAM_LIMIT))
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
    if not _is_str_list(val):
        raise ValueError(f"{key!r} must be a list of strings, got {val!r}")
    return val


def _is_str_list(val: object) -> TypeGuard[list[str]]:
    """Narrow a TOML-decoded value to ``list[str]``: a real ``list`` whose
    every element is a ``str``."""
    if not isinstance(val, list):
        return False
    # Why: a bare-list isinstance on `object` narrows only to list[Unknown],
    # which pyright strict still reports at the iteration; the element check
    # below is what actually establishes list[str].
    items = cast("list[object]", val)
    return all(isinstance(v, str) for v in items)


def _optional_str(cfg: dict[str, Any], key: str, fallback: str | None) -> str | None:
    """Extract an optional str from config or fallback; validate the type."""
    val: Any = cfg.get(key, fallback)
    if val is not None and not isinstance(val, str):
        raise ValueError(f"{key!r} must be a string, got {val!r}")
    return val


def load_workgroup_config(path: Path) -> WorkgroupConfig:
    """Load a workgroup configuration from a TOML file."""
    cfg = WorkgroupConfig.from_toml(path)
    _warn_on_actor_queues_no_child_consumes(cfg, _resolve_actor_registry(cfg.actors))
    return cfg


def _resolve_actor_registry(ref: str) -> Mapping[str, object]:
    """Import ``module:attr`` and return the actor registry it names.

    Resolved once here, by the supervisor, because the alternative is
    silent: every child imports the same reference the moment it is
    spawned, so an unresolvable one crashes each of them at import. The
    supervisor sees only a run of child exits and restarts them on
    backoff until the burst budget is spent, burying the single real
    cause under a cascade of respawns. This is a structural error the
    supervisor can decide locally, so it refuses at load with a message
    naming the reference.
    """
    module_name, _, attr_name = ref.partition(":")
    try:
        # Why no bound on this call: importing the actors module is not an
        # I/O wait on an external party but in-process execution of the
        # application's own module top-level, the same code every child
        # runs at spawn and the worker CLI runs at startup. Python has no
        # safe preemption for module execution: a thread-based deadline
        # cannot interrupt it and would leak a thread still holding the
        # import lock, a strictly worse failure than a hung load. A module
        # whose top-level hangs breaks the application's own startup
        # identically, so this is the explicit exception to the
        # bounded-wait rule.
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ValueError(
            f"actors reference {ref!r} is unresolvable: cannot import "
            f"{module_name!r} ({exc}). Every child would crash on import at "
            "spawn; fix the reference or the module's own import errors."
        ) from exc
    try:
        registry: Any = getattr(module, attr_name)
    except AttributeError as exc:
        raise ValueError(
            f"actors reference {ref!r} is unresolvable: module "
            f"{module_name!r} has no attribute {attr_name!r}. Every child "
            "would crash on import at spawn."
        ) from exc
    if not isinstance(registry, Mapping):
        raise ValueError(
            f"actors reference {ref!r} must name a mapping of actor name to "
            f"actor, got {type(registry).__name__}"
        )
    return cast(Mapping[str, object], registry)


def _warn_on_actor_queues_no_child_consumes(
    cfg: WorkgroupConfig, registry: Mapping[str, Any]
) -> None:
    """Warn, loudly, once, about actors no child of this workgroup serves.

    Never refuses. A workgroup is not the whole fleet: another workgroup,
    another deployment, or a worker started by hand may consume the
    queue, so this supervisor can prove only that *it* does not serve it.
    Refusing would stop a set of children that can do real work over a
    condition the process cannot decide, and a worker able to do work
    never fails to start.

    That makes the log line the only diagnosis there is, which is why it
    names each affected actor and its queue: without it, jobs for that
    actor enqueue successfully and pend forever with no signal anywhere.
    One aggregated event rather than one per actor, the whole registry
    is imported by every child, so per-actor lines would storm exactly
    the split-queue deployments this blesses.
    """
    consumed = {queue for worker in cfg.workers for queue in worker.queues}
    stranded = {
        name: queue
        for name, actor_ref in sorted(registry.items())
        if isinstance(queue := getattr(actor_ref, "queue", None), str) and queue not in consumed
    }
    if not stranded:
        return
    logger.warning(
        "actor-queues-no-child-consumes",
        actors=stranded,
        queues=sorted(set(stranded.values())),
        child_queues=sorted(consumed),
        note=(
            "no child in this workgroup consumes these actors' queues, so "
            "their jobs enqueue and stay pending here. Intended when another "
            "workgroup or deployment consumes the queue, no single supervisor "
            "can know the whole fleet, which is why this never refuses to "
            "start. If nothing consumes it, those jobs never run: add the "
            "queue to some [[workers]] entry's queues."
        ),
    )


def worst_case_shutdown_seconds(settings: WorkerSettings) -> float:
    """The children's modelled worst-case wall clock from SIGTERM to exit.

    Delegates to ``WorkerSettings.worst_case_shutdown_seconds`` (cancellation
    grace + cleanup grace + the bounded-close teardown tail) so the
    supervisor surface and the worker's own model stay one number, no
    drift. Exposed here so ``taskq workgroup validate`` and operators'
    sizing scripts can compare a config's ``shutdown_grace`` against the
    floor it must cover, without importing worker bootstrap internals.
    """
    return settings.worst_case_shutdown_seconds


def _warn_shutdown_grace_window(scfg: SupervisorConfig, settings: WorkerSettings) -> None:
    """Warn when the workgroup's SIGTERM-to-SIGKILL window is too short for
    the children's held release or their clean exit.

    The workgroup forwards SIGTERM to its children, waits
    ``shutdown_grace``, then SIGKILLs. The children spend
    ``cancellation_grace + cleanup_grace`` in the shutdown phases before
    the RELEASING release write: a window shorter than that SIGKILLs the
    child before its held release lands, and the interrupted row rides the
    lease-expiry crash path instead (slower, and it spends the attempt the
    release would have refunded). The floor for a CLEAN child exit is the
    whole ``worst_case_shutdown_seconds`` (graces + bounded-close tail);
    between the two floors the release lands but the SIGKILL truncates the
    exit unwind. The ceiling matters only when the children run with the
    watchdog disabled (no deadline trip bounds their exit): then the
    platform SIGKILL must land by ``termination_grace + exit tail`` or a
    lingering executor thread can outlive a released row's hold: see
    docs/guides/workers.md's platform-grace window.

    Both warnings print the computed numbers (current grace vs the floor
    they miss) so the remedy is a sizing decision, not a research project.
    """
    release_floor = settings.cancellation_grace_period + settings.cleanup_grace_period
    clean_exit_floor = worst_case_shutdown_seconds(settings)
    if scfg.shutdown_grace >= clean_exit_floor:
        return
    if scfg.shutdown_grace >= release_floor:
        logger.warning(
            "workgroup.shutdown_grace_below_clean_exit_floor",
            shutdown_grace=scfg.shutdown_grace,
            release_floor_seconds=release_floor,
            clean_exit_floor_seconds=clean_exit_floor,
            remedy=(
                f"shutdown_grace {scfg.shutdown_grace}s lets a child's held release land "
                f"(release floor {release_floor}s) but still SIGKILLs it before its clean "
                f"exit completes (clean-exit floor {clean_exit_floor}s, the graces plus "
                "the bounded-close teardown tail): the SIGKILL truncates the child's exit "
                "unwind, so terminal writes that needed the tail land via the "
                "lease-expiry crash path instead. Raise supervisor.shutdown_grace to at "
                f"least {clean_exit_floor}s."
            ),
        )
        return
    logger.warning(
        "workgroup.shutdown_grace_below_release_floor",
        shutdown_grace=scfg.shutdown_grace,
        cancellation_grace_period=settings.cancellation_grace_period,
        cleanup_grace_period=settings.cleanup_grace_period,
        release_floor_seconds=release_floor,
        clean_exit_floor_seconds=clean_exit_floor,
        remedy=(
            f"raise supervisor.shutdown_grace to at least {release_floor}s so a "
            "child's held release lands before the SIGKILL (its clean exit "
            f"needs {clean_exit_floor}s (the graces plus the bounded-close "
            "tail); below the floor every interrupted job "
            "rides the lease-expiry crash path and spends the attempt the "
            "release would have refunded"
        ),
    )


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
        if len(w.name) > _MAX_WORKER_NAME_LEN:
            raise ValueError(
                f"worker[{w.name!r}].name must be <= {_MAX_WORKER_NAME_LEN} chars: the "
                "child's health socket path (fixed-length prefix + name + instance-id "
                f"uuid + suffix) must stay within the {_SUN_PATH_BUDGET}-char AF_UNIX "
                "sun_path budget of the tightest supported platform"
            )
        if not w.queues:
            raise ValueError(
                f"worker[{w.name!r}].queues must list at least one queue, a worker "
                "that consumes no queue dispatches nothing; omit the key to fall back "
                "to [defaults].queues, or ['default'] when no default is set"
            )
        if w.pg_credential_provider is not None and (
            not w.pg_credential_provider or ":" not in w.pg_credential_provider
        ):
            # A provider ref the child CLI could never resolve must die
            # HERE, at load: forwarded as-is, the child fails at
            # import-ref resolution before it can register a heartbeat,
            # and the supervisor restart-loops it against the burst budget
            # with the real reason buried in the child's stderr stream.
            raise ValueError(
                f"worker[{w.name!r}].pg_credential_provider must be a "
                "module:attr reference (e.g. 'infra.identity:pg_credentials') "
                f"when present, got {w.pg_credential_provider!r}"
            )
        if w.poll_interval <= 0:
            raise ValueError(f"worker[{w.name!r}].poll_interval must be > 0, got {w.poll_interval}")
        if w.max_concurrency <= 0:
            raise ValueError(
                f"worker[{w.name!r}].max_concurrency must be > 0, got {w.max_concurrency}"
            )
        if w.stream_limit <= 0:
            raise ValueError(f"worker[{w.name!r}].stream_limit must be > 0, got {w.stream_limit}")
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


@dataclass
class _ChildState:
    """Runtime state for one managed child process."""

    spec: WorkerSpec
    process: asyncio.subprocess.Process | None = None
    restart_count: int = 0
    restart_times: list[float] = field(default_factory=list[float])
    instance_id: UUID = field(default_factory=new_uuid)
    backoff: float = 0.0
    restart_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    spawned_at: float = 0.0
    health_failures: int = 0
    stdout_task: asyncio.Task[None] | None = None
    stderr_task: asyncio.Task[None] | None = None
    respawn_task: asyncio.Task[None] | None = None
    """The child's in-flight backoff-then-respawn, run detached so one
    child's backoff cannot pin the monitor's tick (a crash-looping child
    would otherwise blind the monitor to every sibling for up to
    ``backoff_max`` per cycle). The reference prevents garbage collection
    of the detached task."""
    gave_up: bool = False
    """Set when the burst budget is exhausted. The monitor stops
    scheduling this child entirely, the give-up critical fires once,
    never per tick (a 2 Hz critical flood would drown every other
    signal on the box), and a supervisor restart is the operator's
    remedy. Never reset in-process: the window-clearing revival in
    ``_prune_burst`` applies to children still being scheduled."""


def _health_check_sql(schema: str) -> str:
    """Build the health-check query for a worker.

    Freshness is decided server-side, ``last_seen_at`` is written by PG
    (``clock_timestamp()``), so only the server clock can measure its age
    without mixing clock domains; a skewed supervisor clock must not be
    able to read a healthy child as stale (the verdict kills processes).

    The *schema* parameter is validated against _IDENT_RE inline (defence-in-depth
    even though WorkerSettings already constrains it via the regex Field).
    """
    if not _SCHEMA_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        f"SELECT pid, "  # noqa: S608  # Why: schema validated against _SCHEMA_RE immediately above.
        f"(last_seen_at > clock_timestamp() - $3::interval) AS fresh, "
        f"EXTRACT(EPOCH FROM (clock_timestamp() - last_seen_at)) AS age_s "
        f'FROM "{schema}".workers '
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
    The query is bounded by :data:`_HEALTH_QUERY_TIMEOUT_SECS`; a timeout
    counts as one query failure (a client-side deadline is transient ,
    ``taskq.worker._transient``, not evidence the child is hung).
    """
    sql = _health_check_sql(schema)
    try:
        async with pg_pool.acquire(timeout=2.0) as conn:
            # Why wait_for: the query must carry the deadline the acquire
            # already has, without it a black-holed server holds the
            # caller's restart_lock past every other bound in the file.
            # A timeout lands in the except below like any other
            # transient DB failure: logged, counted, healthy until the
            # limit.
            row = await asyncio.wait_for(
                conn.fetchrow(
                    sql, wg_instance, child.spec.name, timedelta(seconds=cfg.stale_after)
                ),
                timeout=_HEALTH_QUERY_TIMEOUT_SECS,
            )
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
    if pid != (child.process.pid if child.process else None):
        logger.debug(
            "workgroup.health_pid_mismatch",
            worker=child.spec.name,
            db_pid=pid,
            local_pid=child.process.pid if child.process else None,
        )
        return False

    fresh: bool | None = row["fresh"]
    if fresh is None:  # last_seen_at IS NULL, never registered a beat
        return False
    if not fresh:
        logger.warning(
            "workgroup.health_stale",
            worker=child.spec.name,
            age_seconds=round(float(row["age_s"] or 0.0), 1),
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
    health_path = _health_socket_path(child.spec.name, child.instance_id)

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
        limit=child.spec.stream_limit,
    )

    child.stdout_task = asyncio.create_task(
        _stream_output(child.process.stdout, child.spec.name, "info")
    )
    child.stderr_task = asyncio.create_task(
        _stream_output(child.process.stderr, child.spec.name, "warning")
    )


async def _read_line(stream: asyncio.StreamReader) -> tuple[bytes, bool]:
    """Read one line, truncating it if it exceeds the reader's limit.

    Returns ``(line, truncated)``; an empty line means EOF.

    Why not ``stream.readline()``: past the limit it raises ``ValueError``
    *and* clears the whole buffer on the way out. Uncaught, that kills
    ``_stream_output`` and with it every subsequent line from that child
    for the rest of the process's life, one 1 MiB JSON log line or a deep
    traceback is enough. ``readuntil()`` reports the same condition as
    ``LimitOverrunError`` while leaving the buffer intact, so the overlong
    line can be drained deliberately (``consumed`` bytes at a time, up to
    the newline) and the lines after it still arrive. Losing one line is
    acceptable; losing the stream is not.
    """
    head = b""
    truncated = False
    while True:
        try:
            line = await stream.readuntil(b"\n")
        except asyncio.IncompleteReadError as exc:  # EOF without a trailing newline
            return (head if truncated else exc.partial), truncated
        except asyncio.LimitOverrunError as exc:
            chunk = await stream.read(exc.consumed) if exc.consumed > 0 else b""
            if not truncated:
                head = chunk
                truncated = True
            # read(n>0) returns empty only at EOF, so this is the "child died
            # mid-line" exit, and it also makes the loop unable to spin.
            if not chunk:
                return head, truncated
        else:
            return (head if truncated else line), truncated


async def _stream_output(
    stream: asyncio.StreamReader | None,
    name: str,
    level: Literal["info", "warning"],
) -> None:
    """Forward child process output lines to the supervisor logger.

    Total by design: an unexpected failure in the pump (transport error,
    decoder fault) is logged loudly, the child's output is a
    supervisor's primary diagnostic surface, and the alternative is an
    unretrieved task exception plus silently lost output for the rest
    of the child's life. Cancellation still propagates: it is the
    shutdown/restart control path, not a failure.
    """
    if stream is None:
        return
    log_fn: Callable[[str], object] = logger.warning if level == "warning" else logger.info
    while True:
        try:
            line, truncated = await _read_line(stream)
            if not line:
                break
            log_fn(
                "workgroup.child_output",
                worker=name,
                line=line.decode(errors="replace").rstrip(),
                truncated=truncated,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "workgroup.stream_pump_failed",
                worker=name,
                level=level,
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
            return


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


def _schedule_restart(
    child: _ChildState,
    scfg: SupervisorConfig,
    *,
    actors: str,
    wg_instance: UUID,
    reason: str,
) -> float | None:
    """Burst-budget check and backoff schedule for one restart attempt.

    Shared by the exit path and the spawn-failure path so a child that
    cannot start at all consumes the same restart budget as one that
    keeps dying, without this, a permanently broken command line would
    retry forever at monitor-tick cadence. Must be called with
    ``child.restart_lock`` held.

    Returns the delay to sleep before the spawn attempt, or ``None`` if
    the burst budget is exhausted and no restart is permitted. Budget
    exhaustion latches ``gave_up``, the caller must not schedule this
    child again (the monitor skips given-up children), so the critical
    fires exactly once per child.
    """
    if not _prune_burst(child, scfg):
        child.gave_up = True
        logger.critical(
            "workgroup-burst-limit-exceeded",
            worker=child.spec.name,
            restarts=len(child.restart_times),
            window_s=scfg.burst_window,
            actors=actors,
            instance_id=str(wg_instance),
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
        actors=actors,
        instance_id=str(wg_instance),
        reason=reason,
    )
    return delay


async def _handle_child_exit(
    child: _ChildState,
    actors: str,
    wg_instance: UUID,
    scfg: SupervisorConfig,
    shutting_down: asyncio.Event,
) -> float | None:
    """React to a child process exiting; compute restart delay.

    Must be called with ``child.restart_lock`` held.  Does **not** sleep or
    spawn, callers must release the lock before sleeping.

    Returns:
        Backoff delay in seconds if a restart should be attempted after
        sleeping, or ``None`` if no restart is needed (burst limit
        exceeded, shutting down, or process already gone).
    """
    proc = child.process
    if proc is None or proc.returncode is None:
        return None

    rc = proc.returncode
    logger.info(
        "workgroup-child-exit",
        worker=child.spec.name,
        exit_code=rc,
        actors=actors,
        instance_id=str(wg_instance),
    )

    child.process = None

    if shutting_down.is_set():
        return None

    return _schedule_restart(
        child, scfg, actors=actors, wg_instance=wg_instance, reason="child_exit"
    )


async def _delay_then_respawn(
    child: _ChildState,
    delay: float,
    actors: str,
    wg_instance: UUID,
    shutting_down: asyncio.Event,
) -> None:
    """Sleep *delay*, racing shutdown, then spawn the replacement.

    Shared by the exit and spawn-failure restart paths: the backoff
    sleep always happens OUTSIDE ``child.restart_lock`` (health checks
    and shutdown must never queue behind a sleeping monitor), a
    shutdown signal interrupts the sleep immediately, the supervisor
    must not sit in backoff for ``backoff_max`` past SIGTERM before it
    even begins forwarding the signal to other children, and the
    spawn re-checks the process slot under the lock.
    """
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
    if shutting_down.is_set():
        return
    async with child.restart_lock:
        if child.process is None:
            try:
                await _spawn_child(child, actors, wg_instance)
            except Exception:
                logger.exception(
                    "workgroup.spawn_failed",
                    worker=child.spec.name,
                )


def _start_respawn(
    child: _ChildState,
    delay: float,
    actors: str,
    wg_instance: UUID,
    shutting_down: asyncio.Event,
) -> None:
    """Run one child's backoff-then-respawn detached from the monitor's tick.

    Awaiting the respawn inline would serialize child-failure detection
    behind one child's backoff sleep: a crash-looping child pinning the
    monitor at ``backoff_max`` per cycle leaves every sibling undetected
    for minutes. Detached, the monitor keeps ticking at its own cadence;
    the respawn task is per-child (the spawn re-checks the process slot
    under the lock, so overlap is safe) and self-terminates on shutdown.
    """
    if child.respawn_task is not None and not child.respawn_task.done():
        return
    child.respawn_task = asyncio.create_task(
        _delay_then_respawn(child, delay, actors, wg_instance, shutting_down),
        name=f"workgroup.respawn.{child.spec.name}",
    )


async def _forward_sighup_to_children(children: Mapping[str, _ChildState]) -> None:
    """Forward SIGHUP to every living child so each child's own credential
    hot-reload runs (the single worker's SIGHUP handler sets its reload
    event; see ``taskq.worker.shutdown.install_signal_handlers``).

    Why the per-child ``restart_lock``: it serializes the send with the
    liveness monitor's exit/respawn path. A child mid-restart (process
    None, or exited and not yet replaced) is skipped rather than signalled:
    its replacement spawns with current credentials anyway, so a reload
    forwarded into the race window is wasted churn at best. A child that
    exits between the liveness check and ``send_signal`` raises
    ``ProcessLookupError``, suppressed for the same reason, the monitor's
    next tick owns that child now.

    Never raises: this runs as a fire-and-forget task off the signal
    handler, an exception here would only surface as a loop-level
    "exception was never retrieved" warning.
    """
    for child in children.values():
        async with child.restart_lock:
            proc = child.process
            if proc is None or proc.returncode is not None:
                continue
            with contextlib.suppress(ProcessLookupError):
                proc.send_signal(signal.SIGHUP)


async def run_forever(config_path: Path) -> None:
    """Load config, spawn children, manage lifecycle until a signal arrives.

    Blocks until SIGTERM or SIGINT, then shuts down all children gracefully.
    SIGHUP is NOT a shutdown signal: it is forwarded to every living child
    so each child's credential hot-reload runs (see
    :func:`_forward_sighup_to_children`); without a handler the default
    disposition would terminate the supervisor and orphan every child.
    Returns normally after shutdown, the CLI caller handles the exit code.
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
                # No WorkerSettings for an explicit health DSN, the module
                # constants (statement_cache_kwargs' fallback) apply.
                stmt_kwargs = statement_cache_kwargs()
            else:
                settings = WorkerSettings.load()
                pg_schema = settings.schema_name
                pg_dsn = str(settings.resolved_pg_dsn_direct)
                stmt_kwargs = statement_cache_kwargs(settings)
            pg_pool = await asyncpg.create_pool(
                pg_dsn,
                min_size=1,
                max_size=len(health_workers) + 1,
                statement_cache_size=stmt_kwargs["statement_cache_size"],
                max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
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
    # Strong refs to the in-flight SIGHUP forward tasks (asyncio only holds
    # weak refs): a dropped task can be garbage-collected mid-flight.
    _sighup_tasks: set[asyncio.Task[None]] = set()

    # ── Emit warnings for risky config ────────────────────────────────
    for w in config.workers:
        if w.force_update_actor_config:
            logger.warning(
                "workgroup.force_update_actor_config_enabled",
                worker=w.name,
                note="Permanent force-update will silently overwrite actor_config on "
                "every restart. Set to false after the first deploy with config changes.",
            )

    # The children inherit this process's environment, so their timing
    # settings are loadable here: the shutdown-grace window warning needs
    # the graces the children will actually run with. Best-effort: a
    # warning must never keep a workgroup from starting.
    with contextlib.suppress(Exception):
        from taskq.settings import WorkerSettings

        _warn_shutdown_grace_window(scfg, WorkerSettings.load())

    # ── Spawn all children initially ──────────────────────────────────
    for child in children.values():
        try:
            await _spawn_child(child, config.actors, wg_instance)
        except Exception:
            logger.exception(
                "workgroup.spawn_failed",
                worker=child.spec.name,
            )
            # Continue, the liveness monitor retries never-spawned
            # children under the same burst/backoff budget as exited
            # ones.

    # ── Signal handler, just sets the event; real cleanup follows ────
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        if not shutting_down.is_set():
            logger.info("workgroup-shutdown-signal")
            shutting_down.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    # SIGHUP, credential rotation: forward to the children, never shut down.
    # Without a handler the default disposition kills the supervisor and
    # orphans every child. The single worker registers its own SIGHUP
    # reload handler (taskq.worker.shutdown.install_signal_handlers), so
    # the operator story is one signal for the whole workgroup: each child
    # runs its own hot-reload. Not available on Windows.
    if hasattr(signal, "SIGHUP"):

        def _on_sighup() -> None:
            # Deliberately await-free: signal handlers run between loop
            # callbacks, the forwarding takes each child's restart_lock
            # inside its own task. Ignored mid-shutdown: reloading pools a
            # child is about to tear down churns resources for nothing.
            if shutting_down.is_set():
                return
            logger.info("workgroup-reload-signal", children=len(children))
            # Referenced until done: a task dropped on the floor can be
            # garbage-collected mid-flight, losing the reload forward.
            task = asyncio.create_task(_forward_sighup_to_children(children))
            _sighup_tasks.add(task)
            task.add_done_callback(_sighup_tasks.discard)

        loop.add_signal_handler(signal.SIGHUP, _on_sighup)

    # ── Liveness monitor, detects exited children and restarts them ──
    async def liveness_monitor() -> None:
        while not shutting_down.is_set():
            for child in list(children.values()):
                if child.gave_up:
                    # Burst budget exhausted: one critical, logged when
                    # the budget refused, then this child is never
                    # scheduled again, the monitor must not re-decide
                    # every tick (a 2 Hz critical flood would drown every
                    # other signal on the box).
                    continue
                proc = child.process
                if proc is None:
                    # A child whose spawn failed (initial or restart) has
                    # no process to observe, without this branch it
                    # would stay dead until the whole supervisor
                    # restarts. Retry it under the same burst/backoff
                    # budget as an exited child.
                    async with child.restart_lock:
                        if child.process is not None or shutting_down.is_set():
                            continue
                        delay = _schedule_restart(
                            child,
                            scfg,
                            actors=config.actors,
                            wg_instance=wg_instance,
                            reason="spawn_failed",
                        )
                    if delay is None:
                        continue
                    _start_respawn(child, delay, config.actors, wg_instance, shutting_down)
                    continue
                if proc.returncode is not None:
                    async with child.restart_lock:
                        # Cancel and reap stale stream tasks from the dead
                        # process; the await reaps the cancellation so no
                        # task is left un-awaited.
                        for t in (child.stdout_task, child.stderr_task):
                            if t is not None and not t.done():
                                t.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await t
                        child.stdout_task = None
                        child.stderr_task = None
                        delay = await _handle_child_exit(
                            child, config.actors, wg_instance, scfg, shutting_down
                        )
                    # Lock released.  The respawn runs detached: the
                    # monitor keeps ticking while this child backs off.
                    if delay is not None:
                        _start_respawn(child, delay, config.actors, wg_instance, shutting_down)
            await asyncio.sleep(0.5)

    # ── Health-check loop, kills hung workers via DB ─────────────────
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
                        logger.warning("workgroup-health-kill", worker=child.spec.name)
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
            logger.error("workgroup-background-task-failed", error=str(exc))
        # Reset child state to a safe baseline before shutdown.
        for child in children.values():
            async with child.restart_lock:
                if child.process is not None and child.process.returncode is not None:
                    child.process = None

    # ── Graceful shutdown ─────────────────────────────────────────────
    logger.info("workgroup-shutdown-begin", grace_s=scfg.shutdown_grace)

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
        # Why bounded: the supervisor's health-pool close is the same
        # dead-PG hang class as worker teardown, an unbounded close
        # would wedge the supervisor between workgroup-shutdown-begin and
        # workgroup-shutdown-complete. The helper never raises and terminates the
        # pool on timeout.
        await close_pool_bounded(pg_pool, "workgroup-health", CLOSE_TIMEOUT_SECS)

    logger.info("workgroup-shutdown-complete")
