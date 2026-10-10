"""``taskq`` CLI entry point.

Usage::

    taskq migrate status
    taskq migrate up [--phase pre|post] [--target VERSION] [--max-steps N] [--ddl-lock-timeout SECS]
    taskq migrate disable-hypertables
    taskq worker --actors myapp.actors:registry
    taskq job show JOB_ID
    taskq job cancel JOB_ID [--reason TEXT]
    taskq job retry JOB_ID
    taskq job cancel-where [--queue NAME] [--status S]... [--dry-run]
    taskq insights [wait|balance|drain|cron|all] [--window 1h|6h|24h|7d] [--actor A] [--queue NAME]

The console script puts the current working directory on ``sys.path``
(see :func:`main`), so ``module:attr`` options resolve application modules
from the directory the operator ran the command in.
"""

import asyncio
import contextlib
import importlib
import os
import re
import signal
import sys
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Final, cast
from uuid import UUID

import asyncpg
import structlog
import typer

from taskq import migrate as migrate_mod
from taskq._advisory import DEADLINE_ERRORS
from taskq._close import (
    CLOSE_TIMEOUT_SECS,
    close_conn_bounded,
    close_pool_bounded,
    close_provider_bounded,
    close_redis_bounded,
)
from taskq._doctor import (
    _DOCTOR_INSIGHTS_WINDOW,  # pyright: ignore[reportPrivateUsage]  # Why: the doctor analysis engine moved to taskq._doctor; the CLI orchestrator reads the window constant and the test suite pins it through this module.
    _doctor_findings,  # pyright: ignore[reportPrivateUsage]  # Why: the finding families are pure analysis living in taskq._doctor; _doctor below stays the renderer, so the CLI boundary the tests patch is unchanged.
    _StrandedActorJobs,  # pyright: ignore[reportPrivateUsage]  # Why: the stranded-jobs record is shared by the CLI's asyncpg fetcher (_list_stranded_pending_jobs) and the moved engine; cli.py is the surviving call path.
    _unknown_taskq_env_vars,  # pyright: ignore[reportPrivateUsage]  # Why: tests import this scan from taskq.cli and _doctor calls it here; the engine module only hosts the analysis.
)
from taskq._forkguard import guarded_connection_class
from taskq._humantime import humanize_age
from taskq._json import loads as json_loads
from taskq._reaper import reap_cancelled_child
from taskq.actor import ActorRef
from taskq.actor_config_ops import (
    UNSET,
    ActorConfigRow,
    ActorQueueMoveResult,
    Unset,
    deregister_actor,
    get_actor_config,
    list_actor_configs,
    move_actor_queue,
    set_actor_config_capacity,
)
from taskq.auth import (
    PgCredentialProvider,
    RedisCredentialProvider,
    ReloadSchedule,
    build_worker_connections,
    make_dedicated_conn_factory,
    make_pg_pool_factory,
    make_redis_client_factory,
    reload_schedule_of,
)
from taskq.backend._filter_sql import (
    build_filter_conditions,  # pyright: ignore[reportPrivateUsage]  # Why: the one filter-to-WHERE builder shared by the client's list and cancel_where; the cancel-where dry-run must preview the exact predicate set the write will apply, so re-deriving the conditions in the CLI would let the two drift.
)
from taskq.backend._protocol import JobFilter, JobId, parse_retry_kind
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client import TaskQ
from taskq.connections import ConnFactory, PoolFactory, RedisFactory, WorkerConnections
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex for defence-in-depth schema validation at this SQL interpolation site, the queue_ops convention.
    require_schema,
)
from taskq.exceptions import (
    ActorConfigDriftList,
    ActorDeregistrationError,
    ActorNotFoundError,
    EmptyFilterError,
)
from taskq.insights import (
    INSIGHTS_WINDOWS,
    fetch_cron_ledger,
    fetch_drain_estimates,
    fetch_overprovisioning,
    fetch_queue_imbalance,
    fetch_wait_distribution,
)
from taskq.obs import OtelExporterConfigurationError, configure_exporters, setup_logging
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.timescale import (
    StorageMode,
    TimescaleDBUnavailableError,
    detect_storage_mode,
    disable_hypertables,
    enable_hypertables,
    probe_registered_policy_jobs,
)
from taskq.types import BulkCancelResult
from taskq.worker.dev import dev_watch_loop
from taskq.worker.health import default_worker_health_socket_path
from taskq.worker.queue_ops import (
    QUEUE_MODES,
    QueueRow,
    get_queue,
    list_queues,
    set_queue_max_concurrent,
    set_queue_mode,
)
from taskq.worker.run import worker_main as _worker_main

logger: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.cli")

# ── UI first-use bounds (client/CLI processes arm no watchdogs) ────────
#
# The worker sources these bounds from WorkerSettings
# (reload_factory_timeout, health_pg_ping_timeout,
# dispatcher_command_timeout), but `taskq ui serve` loads TaskQSettings ,
# the base class carries none of them. The literals mirror those defaults
# exactly; module-level so tests shrink them as seams (the
# CLOSE_TIMEOUT_SECS convention).
_UI_FACTORY_TIMEOUT_SECS: Final[float] = 30.0
"""Bounds the UI's first-use awaits, ``pool_factory()`` /
``redis_factory()`` (the AAD first token fetch lives inside them) and the
eager redis ``initialize()`` (the first broker round trip). Mirrors
``WorkerSettings.reload_factory_timeout``'s default, the SAME bound the
worker applies to every factory call it makes (worker/deps.py), not a
second mechanism. A hung dependency fails UI startup loudly instead of
parking the admin server forever."""

_UI_PG_PING_TIMEOUT_SECS: Final[float] = 0.2
"""Bounds the ``/jobs/health/ready`` PG probe (acquire + SELECT 1).
Mirrors ``WorkerSettings.health_pg_ping_timeout``'s default, the bound
the worker's readiness ping (worker/health.py) applies to the identical
probe; an unbounded probe turns a wedged PG into a wedged prober."""

_UI_POOL_COMMAND_TIMEOUT_SECS: Final[float] = 5.0
"""Per-query ``command_timeout`` for the UI's admin pool. Mirrors
``WorkerSettings.dispatcher_command_timeout``'s default, the per-query
bound on every other pool the repo builds. One pool-level bound covers
every admin-page query on the pool (the sweep's ~20 admin query sites);
without it a black-holed PG wedges each admin request forever."""

app = typer.Typer(
    name="taskq",
    no_args_is_help=True,
    help="TaskQ, async Postgres-backed background jobs.",
)
migrate_app = typer.Typer(no_args_is_help=True, help="Apply or inspect schema migrations.")
app.add_typer(migrate_app, name="migrate")

worker_app = typer.Typer(help="Run a TaskQ worker.")
app.add_typer(worker_app, name="worker")

health_app = typer.Typer(no_args_is_help=True, help="Probe the worker's health endpoints.")
app.add_typer(health_app, name="health")

ui_app = typer.Typer(no_args_is_help=True, help="Admin UI server.")
app.add_typer(ui_app, name="ui")

workgroup_app = typer.Typer(
    no_args_is_help=True,
    help="Manage a multi-worker process group (supervisor).",
)
app.add_typer(workgroup_app, name="workgroup")

actor_config_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect and tune stored actor_config capacity fields on a live deployment.",
)
app.add_typer(actor_config_app, name="actor-config")

queues_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect and configure queue dispatch mode and per-queue concurrency caps.",
)
app.add_typer(queues_app, name="queues")

# Queue-lifecycle operations. An operator moving an actor between queues is
# thinking about queues, not about the actor_config table the assignment
# happens to live in, so the move is reachable under this noun as well as
# under `actor-config`.
queue_app = typer.Typer(
    no_args_is_help=True,
    help="Queue lifecycle operations.",
)
app.add_typer(queue_app, name="queue")

job_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect individual jobs and operate on them (cancel, retry, bulk cancel).",
)
app.add_typer(job_app, name="job")

# The workflow-run surface (T12): one question, one command — stuck →
# status, deliver → signal, reply → resolve, stop → cancel, retry-a-
# failed-node → retry, inventory → list. The analysis lives in
# taskq.workflows._cli (the _doctor seam); the write verbs call the
# engine's module-level run-operator functions (one engine, two surfaces).
flows_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect and operate on workflow runs (status, holds, resolve, cancel, retry).",
)
app.add_typer(flows_app, name="flows")


def _import_ref(ref: str, *, example: str) -> Any:
    """Resolve a ``module:attr`` reference to the attribute it names.

    The single dotted-path resolver behind every ``module:attr`` CLI
    option (``--actors``, the credential-provider options). On any failure
    it prints the reason to stderr and raises ``typer.Exit(code=1)``:
    every caller wires a resource the process cannot run without, so a
    bad reference is fatal at startup rather than a degraded fallback.
    """
    module_name, sep, attr_name = ref.partition(":")
    if not sep or not module_name or not attr_name:
        typer.echo(
            f"expected module:attr syntax (e.g. {example}); got {ref!r}",
            err=True,
        )
        raise typer.Exit(code=1)

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        typer.echo(f"module not found: {module_name}", err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(f"failed to import module {module_name}: {exc}", err=True)
        raise typer.Exit(code=1) from None

    try:
        return getattr(module, attr_name)
    except AttributeError:
        typer.echo(
            f"attribute {attr_name!r} not found in module {module_name}",
            err=True,
        )
        raise typer.Exit(code=1) from None


def _load_actor_registry(actors: str) -> Mapping[str, ActorRef[Any, Any]]:
    """Resolve a ``module:attr`` reference to an actor registry.

    Accepts either ``Mapping[str, ActorRef]`` or an iterable of
    ``ActorRef`` (keyed by name). An iterable is materialized once,
    before the validation pass reads it: a one-shot iterator consumed
    by validation cannot then be rebuilt into the registry it proved it
    held. An empty registry is refused rather than returned, every
    downstream consumer checks ``is not None`` and cannot distinguish
    ``{}`` from a populated mapping, so a worker handed an empty
    registry boots and dispatches nothing. On any failure prints the
    reason to stderr and raises ``typer.Exit(code=1)``, shared by
    ``worker`` and ``actor-config diff``.

    That sharing is a deliberate trade-off for the read-only ``diff``
    command: it too exits 1 on an empty registry, because the shared
    loader cannot tell "an operator auditing stored rows against an
    intentionally empty registry" from "a misconfigured ``--actors``
    ref that resolved to nothing", and the second is far more likely.
    The workaround for a legitimate empty-registry audit: point
    ``--actors`` at a populated registry containing nothing of interest
    to the comparison, or read the stored rows directly
    (``taskq actor-config list``).
    """
    raw = _import_ref(actors, example="myapp.actors:registry")

    registry: Mapping[str, ActorRef[Any, Any]]
    if isinstance(raw, Mapping):
        registry = cast(Mapping[str, ActorRef[Any, Any]], raw)
    elif not isinstance(raw, (str, bytes)) and hasattr(raw, "__iter__"):
        # Unvalidated until the isinstance guard below passes, so the
        # element type is Any here, annotating ActorRef would make the
        # guard look dead to the type checker.
        items: list[Any] = list(raw)
        if not all(isinstance(r, ActorRef) for r in items):
            typer.echo(
                "expected Mapping[str, ActorRef] or Iterable[ActorRef] at "
                f"{actors}; got {type(raw).__name__}",
                err=True,
            )
            raise typer.Exit(code=1)
        # Duplicate names are refused, not collapsed. A dict comprehension
        # silently let the LAST duplicate win: two distinct handlers
        # registered under one name booted with actor_count == 1 and every
        # dispatch for the loser ran the winner's code, the worst failure
        # mode a registry can have (the wrong function executes, no
        # symptom anywhere). The duplicate name is a programming error,
        # refused at load: the message names the collision and BOTH
        # registration sites (each handler's module + qualname), the same
        # loud-refusal contract as the empty-registry guard below. The
        # exact-same ActorRef object listed twice is NOT a collision: the
        # dispatch behavior is byte-identical (one function, one config),
        # so collapsing it preserves the old shape for restart/registry-
        # rebuild flows that may re-list an entry.
        registry = {}
        for ref in items:
            existing = registry.get(ref.name)
            if existing is not None and existing is not ref:
                typer.echo(
                    f"duplicate actor name {ref.name!r} at {actors}: "
                    f"{existing.fn.__module__}.{existing.fn.__qualname__} and "
                    f"{ref.fn.__module__}.{ref.fn.__qualname__} both register it; "
                    "dispatch would silently run one of them for the other's "
                    "jobs, refusing to boot",
                    err=True,
                )
                raise typer.Exit(code=1)
            registry[ref.name] = ref
    else:
        typer.echo(
            "expected Mapping[str, ActorRef] or Iterable[ActorRef] at "
            f"{actors}; got {type(raw).__name__}",
            err=True,
        )
        raise typer.Exit(code=1)

    if not registry:
        typer.echo(
            f"actor registry at {actors} is empty, a worker with no actors "
            "dispatches nothing; refusing to boot",
            err=True,
        )
        raise typer.Exit(code=1)
    return registry


_PROVIDER_EXAMPLE: Final[str] = "myapp.auth:make_provider"


def _resolve_provider(ref: str, *, option: str, method: str) -> Any:
    """Resolve a ``module:attr`` reference to a credential provider.

    Accepts the same shapes an application naturally exports: a provider
    **instance** (``myapp.auth:PROVIDER``), a zero-arg **factory**
    returning one (``myapp.auth:make_provider``), or the provider
    **class** itself when its constructor takes no required arguments ,
    the same "resolve the ref, then adapt the accepted shapes" contract
    :func:`_load_actor_registry` uses for ``--actors``.

    Anything else is fatal: a credential path that silently fell back to
    the DSN would authenticate with a static password and look healthy
    until the first reconnect after the deploy.
    """
    obj = _import_ref(ref, example=_PROVIDER_EXAMPLE)
    if isinstance(obj, type) or (not hasattr(obj, method) and callable(obj)):
        try:
            obj = obj()
        except Exception as exc:
            typer.echo(f"{option}: calling {ref} raised {type(exc).__name__}: {exc}", err=True)
            raise typer.Exit(code=1) from None
    if not callable(getattr(obj, method, None)):
        typer.echo(
            f"{option}: {ref} resolved to {type(obj).__name__}, which does not implement "
            f"async {method}(). Provide a credential provider instance, a zero-arg factory "
            "returning one, or the provider class, see docs/guides/managed-identities.md.",
            err=True,
        )
        raise typer.Exit(code=1)
    return obj


def _load_pg_credential_provider(ref: str, *, option: str) -> PgCredentialProvider:
    return cast(
        PgCredentialProvider, _resolve_provider(ref, option=option, method="get_pg_credential")
    )


def _load_redis_credential_provider(ref: str, *, option: str) -> RedisCredentialProvider:
    return cast(
        RedisCredentialProvider,
        _resolve_provider(ref, option=option, method="get_redis_credential"),
    )


def _resolved_ref(flag: str | None, configured: str | None) -> str | None:
    """Resolve a credential-provider ref: explicit CLI flag beats settings.

    The refs are ordinary settings, so they take part in dotenvmodel's
    ``.env`` cascade like everything else; the explicit flag beats the
    configured value.
    """
    return flag if flag is not None else configured


def _credential_connections(
    settings: WorkerSettings,
    pg_ref: str | None,
    redis_ref: str | None,
) -> tuple[WorkerConnections | None, PgCredentialProvider | None]:
    """Build the worker's provider-backed connections, or ``None`` when unset.

    ``None`` keeps the DSN path exactly as it was, so the hook is purely
    additive; a misconfiguration on either side exits non-zero at startup
    instead of starting a worker whose SIGHUP rotates nothing.

    Returns the resolved Postgres provider alongside (``None`` when no
    PG ref is set), the worker-internal per-slot transaction pool does
    not read WorkerConnections, so the provider object is handed to the
    worker separately and must not be resolved twice.
    """
    if pg_ref is None and redis_ref is None:
        return None, None
    pg_provider = (
        _load_pg_credential_provider(pg_ref, option="--pg-credential-provider")
        if pg_ref is not None
        else None
    )
    redis_provider = (
        _load_redis_credential_provider(redis_ref, option="--redis-credential-provider")
        if redis_ref is not None
        else None
    )
    try:
        conns = build_worker_connections(
            settings, pg_provider=pg_provider, redis_provider=redis_provider
        )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    return conns, pg_provider


def _credential_conn_factory(dsn: str, pg_ref: str | None, *, option: str) -> ConnFactory | None:
    """Build a one-shot dedicated-connection factory for the DSN-only commands.

    ``taskq migrate`` and ``taskq ui serve`` open their own connections;
    without this they are the two places a managed-identity deployment
    still needs a static password.
    """
    if pg_ref is None:
        return None
    provider = _load_pg_credential_provider(pg_ref, option=option)
    return make_dedicated_conn_factory(dsn, provider)


@worker_app.callback(invoke_without_command=True)
def worker(
    actors: str = typer.Option(
        ...,
        "--actors",
        help="Module:attr reference to the actor registry (e.g. myapp.actors:registry). "
        "Resolves at startup to a Mapping[str, ActorRef] or Iterable[ActorRef].",
    ),
    force_update_actor_config: bool = typer.Option(
        False,
        "--force-update-actor-config",
        help="Allow sync_actor_config to overwrite a stored actor_config row whose "
        "metadata differs from the registered value. Use for one deploy to "
        "deliberately adopt a code-side metadata change, then unset. The queue "
        "assignment is unaffected, boots never rewrite it; move an actor with "
        "`taskq actor-config move-queue`. Capacity fields (max_concurrent / "
        "max_pending / result_ttl) are likewise unaffected, use `taskq "
        "actor-config set` for those. Equivalent to env var "
        "TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true.",
    ),
    queues: list[str] | None = typer.Option(
        None,
        "--queues",
        help="Queue names to consume from (repeat the flag once per queue). Overrides TASKQ_QUEUES.",
    ),
    max_concurrency: int | None = typer.Option(
        None,
        "--max-concurrency",
        help="Upper bound on concurrent jobs. Overrides TASKQ_MAX_CONCURRENCY.",
    ),
    poll_interval: float | None = typer.Option(
        None,
        "--poll-interval",
        help="Producer loop fallback polling cadence in seconds. Overrides TASKQ_POLL_INTERVAL.",
    ),
    worker_group: str | None = typer.Option(
        None,
        "--worker-group",
        help="Consumer group name for observability spans. Overrides TASKQ_WORKER_GROUP.",
    ),
    worker_label: str | None = typer.Option(
        None,
        "--worker-label",
        help="Human-readable label stored in the workers table for correlation "
        "with workgroup supervisors and external monitoring.",
    ),
    workgroup_instance: str | None = typer.Option(
        None,
        "--workgroup-instance",
        help="UUIDv7 identifying the workgroup orchestrator that launched "
        "this worker. Used for cross-process correlation and health checking.",
    ),
    health_socket_path: str | None = typer.Option(
        None,
        "--health-socket-path",
        help="Unix socket path for the health server. Overrides TASKQ_HEALTH_SOCKET_PATH. "
        "Use unique paths when running multiple workers on the same host.",
    ),
    until_idle: bool = typer.Option(
        False,
        "--until-idle",
        help="Run until all subscribed queues are drained, then exit. "
        "Exit 0 if all jobs succeeded, 3 if any failed, 4 if idle-max-runtime "
        "was exceeded. Incompatible with cron-driven workloads.",
    ),
    idle_settle_window: float | None = typer.Option(
        None,
        "--idle-settle-window",
        help="Seconds to wait after queues appear empty before declaring "
        "drained. Overrides TASKQ_IDLE_SETTLE_WINDOW. Default 2.0. "
        "Only used with --until-idle.",
    ),
    idle_poll_interval: float | None = typer.Option(
        None,
        "--idle-poll-interval",
        help="How often to check queue depth. Overrides TASKQ_IDLE_POLL_INTERVAL. "
        "Default 1.0. Only used with --until-idle.",
    ),
    idle_max_runtime: float | None = typer.Option(
        None,
        "--idle-max-runtime",
        help="Maximum wall-clock seconds before forcing exit (code 4). "
        "Overrides TASKQ_IDLE_MAX_RUNTIME. Only used with --until-idle.",
    ),
    pg_credential_provider: str | None = typer.Option(
        None,
        "--pg-credential-provider",
        help="Module:attr reference to a PgCredentialProvider (e.g. "
        f"{_PROVIDER_EXAMPLE}), an instance, a zero-arg factory returning one, "
        "or the provider class. Every Postgres pool and dedicated connection is "
        "then built through it, so SIGHUP / TASKQ_RELOAD_INTERVAL rotate real "
        "credentials. Overrides TASKQ_PG_CREDENTIAL_PROVIDER (inherited by "
        "workgroup-supervised workers) via dotenvmodel.",
    ),
    redis_credential_provider: str | None = typer.Option(
        None,
        "--redis-credential-provider",
        help="Module:attr reference to a RedisCredentialProvider, in the same "
        "shapes as --pg-credential-provider. Requires TASKQ_REDIS_URL. "
        "Overrides TASKQ_REDIS_CREDENTIAL_PROVIDER via dotenvmodel.",
    ),
) -> None:
    """Start a TaskQ worker consuming from the given actor registry."""
    registry = _load_actor_registry(actors)

    settings = WorkerSettings.load()
    if force_update_actor_config:
        settings.force_update_actor_config = True
    if queues is not None:
        settings.queues = queues
    if max_concurrency is not None:
        settings.max_concurrency = max_concurrency
    if poll_interval is not None:
        settings.poll_interval = poll_interval
    if worker_group is not None:
        settings.worker_group = worker_group
    if worker_label is not None:
        settings.worker_label = worker_label
    if workgroup_instance is not None:
        settings.workgroup_instance = workgroup_instance
    if health_socket_path is not None:
        settings.health_socket_path = health_socket_path
    elif WorkerSettings.resolve_cascade_value("TASKQ_HEALTH_SOCKET_PATH") is None:
        # No explicit configuration anywhere (this option, the process
        # environment, the .env cascade): the worker binds the
        # per-worker-unique default instead of the static
        # ``/tmp/taskq_health.sock``. That static value was a global
        # collision point: two workers on one host left the second with no
        # socket at all (its ``health-server-unavailable`` warning, boot
        # continuing) while ``taskq health`` silently answered with the
        # FIRST worker's state, the cross-report that made the worker that
        # just warned look healthy. A pid-unique path makes every
        # co-located worker own its socket; an explicit value here or in
        # the cascade stays authoritative verbatim, including one spelled
        # as the old default (that operator asked for the shared path and
        # keeps its collision warning).
        settings.health_socket_path = default_worker_health_socket_path()
    connections, pg_provider = _credential_connections(
        settings,
        _resolved_ref(pg_credential_provider, settings.pg_credential_provider),
        _resolved_ref(redis_credential_provider, settings.redis_credential_provider),
    )

    # Exporters are wired here, before worker_main records anything:
    # measurements a proxy instrument takes before an SDK provider exists
    # are dropped, not replayed. Logging is configured first (the same
    # idempotent setup worker_main repeats) so the wiring's startup line
    # renders in the operator's configured format.
    setup_logging(
        level=settings.log_level,
        log_format=settings.log_format,
        events_level=settings.log_events_level,
    )
    try:
        configure_exporters(settings)
    except OtelExporterConfigurationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    try:
        code = _worker_main(
            settings,
            actor_registry=registry,
            connections=connections,
            pg_credential_provider=pg_provider,
            until_idle=until_idle,
            idle_settle_window=idle_settle_window,
            idle_poll_interval=idle_poll_interval,
            idle_max_runtime=idle_max_runtime,
        )
    except ActorConfigDriftList as e:
        # Why: the remedy hint is folded into ActorConfigDriftList.__str__
        # itself (see exceptions.py), don't print it a second time here.
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1) from None
    # The loop is closed (asyncio restored SIG_DFL on close), but the
    # process is not gone yet: the exit path, interpreter teardown, atexit
    # hooks and thread joins all still run, and a loaded host stretches
    # every one of them. A signal in that window kills the process with
    # -15 and ERASES the drain's verdict (the observed fleet-wide-storm
    # shape - the orchestrator's redundant stop-signal landing after one
    # pod had already drained cleanly). The escalation contract only has
    # meaning while the loop lives; once it is closed there is nothing
    # left to escalate, so SIGTERM is ignored and the exit status stays
    # the drain's. This guard lives HERE - the process is genuinely about
    # to die - and not in ``worker_main``: an ignored disposition set in a
    # library function is inherited by every process that host forks
    # afterwards (the proven -9 leak), while an entrypoint's next act is
    # its own exit.
    with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise typer.Exit(code=code)


@app.command("dev", help="Development utilities.")
def dev_watch(
    actors: Annotated[str, typer.Argument(help="Import path: dotted.module:attr")],
    watch: Annotated[
        list[str] | None,
        typer.Option("--watch", help="Path to watch (repeatable). Default: cwd."),
    ] = None,
    grace_period: Annotated[
        int,
        typer.Option("--grace-period", min=0, help="Seconds before SIGKILL. Default: 5."),
    ] = 5,
) -> None:
    """Run a worker in dev mode with auto-reload on file changes."""
    module_name, sep, attr_name = actors.partition(":")
    if not sep or not module_name or not attr_name:
        typer.echo(
            f"expected module:attr syntax (e.g. myapp.actors:registry); got {actors!r}",
            err=True,
        )
        raise typer.Exit(code=1)

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        typer.echo(f"Error: cannot import '{module_name}', module not found", err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(f"Error: cannot import '{module_name}', {exc}", err=True)
        raise typer.Exit(code=1) from None

    try:
        getattr(module, attr_name)
    except AttributeError:
        typer.echo(
            f"Error: attribute {attr_name!r} not found in module {module_name}",
            err=True,
        )
        raise typer.Exit(code=1) from None

    watch_paths: list[str] = list(watch) if watch else [str(Path.cwd())]

    watch_display = ", ".join(str(p) for p in watch_paths)
    typer.echo(f"TaskQ dev mode, watching {watch_display}. Press Ctrl-C to stop.", err=True)

    with asyncio.Runner() as runner:
        runner.run(
            dev_watch_loop(actors, watch_paths=watch_paths, grace_period=float(grace_period))
        )


_CONNECT_TIMEOUT_S: Final[float] = 0.1
_REQUEST_TIMEOUT_S: Final[float] = 2.0


@migrate_app.command("status")
def migrate_status(
    pg_credential_provider: str | None = typer.Option(
        None,
        "--pg-credential-provider",
        help="Module:attr reference to a PgCredentialProvider (e.g. "
        f"{_PROVIDER_EXAMPLE}). The connection is opened through it instead of "
        "the DSN's static password. Overrides TASKQ_PG_CREDENTIAL_PROVIDER.",
    ),
) -> None:
    """Show applied and pending migrations."""
    settings = TaskQSettings.load()
    conn_factory = _credential_conn_factory(
        str(settings.pg_dsn),
        _resolved_ref(pg_credential_provider, settings.pg_credential_provider),
        option="--pg-credential-provider",
    )
    asyncio.run(_status(settings, conn_factory=conn_factory))


@migrate_app.command("up")
def migrate_up(
    phase: migrate_mod.Phase | None = typer.Option(
        None, "--phase", help="Restrict to 'pre' or 'post'."
    ),
    target: str | None = typer.Option(
        None, "--target", help="Stop after this version (inclusive). E.g. 01.00.00_01"
    ),
    max_steps: int | None = typer.Option(None, "--max-steps", help="Cap number of applies."),
    ddl_lock_timeout: float = typer.Option(
        migrate_mod.DEFAULT_MIGRATION_DDL_LOCK_TIMEOUT,
        "--ddl-lock-timeout",
        min=0.0,
        help="Seconds a transactional migration waits for a table lock before it "
        "fails and rolls back (SET LOCAL lock_timeout). 0 waits indefinitely, "
        "parking every statement on the table behind the queued DDL.",
    ),
    allow_checksum_drift: bool = typer.Option(
        False,
        "--allow-checksum-drift",
        help="Proceed past a drifted checksum ledger: an APPLIED migration's "
        "ledger checksum differs from the bundled file, which refuses the run "
        "by default. Verify the edit is safe first; the drift is still logged "
        "as a warning on every run and the ledger keeps the stored checksum.",
    ),
    pg_credential_provider: str | None = typer.Option(
        None,
        "--pg-credential-provider",
        help="Module:attr reference to a PgCredentialProvider (e.g. "
        f"{_PROVIDER_EXAMPLE}). The connection is opened through it instead of "
        "the DSN's static password. Overrides TASKQ_PG_CREDENTIAL_PROVIDER.",
    ),
) -> None:
    """Apply pending migrations."""
    settings = TaskQSettings.load()
    conn_factory = _credential_conn_factory(
        str(settings.pg_dsn),
        _resolved_ref(pg_credential_provider, settings.pg_credential_provider),
        option="--pg-credential-provider",
    )
    asyncio.run(
        _up(
            settings,
            phase=phase,
            target=target,
            max_steps=max_steps,
            ddl_lock_timeout=ddl_lock_timeout,
            allow_checksum_drift=allow_checksum_drift,
            conn_factory=conn_factory,
        )
    )


async def _open_migrate_conn(
    settings: TaskQSettings, conn_factory: ConnFactory | None
) -> asyncpg.Connection:
    """Open the one-shot connection the migrate commands run on.

    ``conn_factory`` (built from --pg-credential-provider) fetches a fresh
    credential; ``None`` keeps the DSN path.
    """
    if conn_factory is not None:
        return await conn_factory()
    return await asyncpg.connect(str(settings.pg_dsn))


async def _status(settings: TaskQSettings, *, conn_factory: ConnFactory | None = None) -> None:
    conn = await _open_migrate_conn(settings, conn_factory)
    try:
        applied = await migrate_mod.list_applied(conn, settings.schema_name)
        # checksum_drifts' docstring promises status shares the drift report
        # with migrate up's refusal — an operator checking a drifted ledger
        # must see it HERE, not discover it as a deploy-time refusal. The
        # connection is the one this command owns; the call is read-only.
        drifts = await migrate_mod.checksum_drifts(conn, schema=settings.schema_name)
    finally:
        # Why bounded: a dead PG can block close() indefinitely, wedging even
        # this one-shot command before process exit. The
        # helper terminates on timeout and never raises, so a close error cannot
        # mask an in-flight exception from list_applied.
        await close_conn_bounded(conn, "migrate-status", CLOSE_TIMEOUT_SECS)
    typer.echo(f"schema: {settings.schema_name}")
    typer.echo(f"applied: {len(applied)}")
    for migration in migrate_mod.discover():
        marker = "✔" if migration.key in applied else " "
        suffix = "" if migration.use_transaction else " (no transaction)"
        if migration.key in drifts:
            drift = drifts[migration.key]
            marker = "✗"
            suffix += (
                f"  CHECKSUM DRIFT: ledger {drift.stored[:12]} != file {drift.current[:12]}"
                " — the next migrate up will refuse; see upgrading.md"
            )
        typer.echo(f"  [{marker}] {migration.filename}{suffix}")
    if drifts:
        typer.echo(
            f"CHECKSUM DRIFT: {len(drifts)} applied migration(s) no longer match the bundled files"
        )
        for key, drift in sorted(drifts.items()):
            typer.echo(f"  {key}: ledger {drift.stored[:12]} != file {drift.current[:12]}")


async def _up(
    settings: TaskQSettings,
    *,
    phase: migrate_mod.Phase | None,
    target: str | None,
    max_steps: int | None,
    ddl_lock_timeout: float = migrate_mod.DEFAULT_MIGRATION_DDL_LOCK_TIMEOUT,
    allow_checksum_drift: bool = False,
    conn_factory: ConnFactory | None = None,
) -> None:
    # Why locked: the README names `taskq migrate up` as THE deploy step, and a
    # container platform will start two replicas or retry a failed job, so "run
    # it once, sequentially" is not something the caller can guarantee.
    # Unlocked, the loser of a concurrent race against a virgin schema hits a
    # bare CREATE TABLE (the pre-initial migration has no IF NOT EXISTS) and
    # crash-loops on DuplicateTableError.
    #
    # Why the lock is taken here rather than by delegating to
    # apply_pending_locked: this path owns the connection so it can run
    # _report_up_failure diagnostics on it AFTER a failure, and
    # apply_pending_locked converts failures into SystemExit before that could
    # run. Both paths serialize on the same advisory lock via the shared
    # migration_advisory_lock helper, so there is no second lock protocol.
    #
    # conn mirrors apply_pending_locked's conn-or-None pattern: connect
    # failures land in the same guarded region as apply failures, so both
    # get the report, and the close is skipped when no conn was acquired.
    conn: asyncpg.Connection | None = None
    try:
        conn = await _open_migrate_conn(settings, conn_factory)
        async with migrate_mod.migration_advisory_lock(conn, schema=settings.schema_name):
            applied = await migrate_mod.apply_pending(
                conn,
                schema=settings.schema_name,
                phase=phase,
                target=target,
                max_steps=max_steps,
                ddl_lock_timeout=ddl_lock_timeout,
                allow_checksum_drift=allow_checksum_drift,
            )
            # The optional hypertable conversion rides the same deploy step
            # and the same lock: after migrations, before the connection
            # closes. With TASKQ_TIMESCALEDB_HYPERTABLES false (the default)
            # this is a zero-statement gate; with it true, enable_hypertables
            # refuses loudly when the server cannot honor the feature.
            # WorkerSettings.load() re-reads the same cascade: the retention
            # intervals the conversion derives chunk sizes and policies from
            # are worker-scoped fields, so the deploy step reads them from
            # the environment the workers themselves run with.
            if settings.timescaledb_hypertables:
                await enable_hypertables(
                    conn, schema=settings.schema_name, settings=WorkerSettings.load()
                )
    except TimescaleDBUnavailableError as exc:
        # A capability refusal, not a migration failure: _report_up_failure
        # would misfile it as broken schema state. The error already names
        # the setting and what the server is missing.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    except SystemExit as exc:
        # Lock contention. Already a precise message; reporting it through
        # _report_up_failure would misfile a queueing problem as a broken
        # migration and print schema diagnostics for a schema that is fine.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        # Why diagnose here: both apply paths leave the connection reusable
        # (a transactional failure rolls back; the no-transaction path never
        # opened one), so the CLI reports what failed and the schema state
        # on the same conn instead of escaping a raw traceback.
        await _report_up_failure(conn, settings.schema_name, exc)
        raise typer.Exit(code=1) from None
    finally:
        if conn is not None:
            # Why bounded: same dead-PG wedge risk as _status above;
            # terminate-on-timeout, never raises.
            await close_conn_bounded(conn, "migrate-up", CLOSE_TIMEOUT_SECS)
    if not applied:
        typer.echo("no pending migrations")
        return
    typer.echo(f"applied {len(applied)} migration(s):")
    for migration in applied:
        typer.echo(f"  {migration.filename}")


@migrate_app.command("disable-hypertables")
def migrate_disable_hypertables(
    pg_credential_provider: str | None = typer.Option(
        None,
        "--pg-credential-provider",
        help="Module:attr reference to a PgCredentialProvider (e.g. "
        f"{_PROVIDER_EXAMPLE}). The connection is opened through it instead of "
        "the DSN's static password. Overrides TASKQ_PG_CREDENTIAL_PROVIDER.",
    ),
) -> None:
    """Convert the TimescaleDB hypertables back to plain vanilla tables.

    The disable mirror of the ``migrate up`` deploy step's conversion: every
    registered retention/compression policy is removed, and per hypertable the
    rows return into a table whose shape is the bundled migrations' own output
    (count- and twin-verified at every hand-off — see
    ``docs/guides/timescaledb.md``). Flip ``TASKQ_TIMESCALEDB_HYPERTABLES``
    off FIRST: with the flag still true this refuses loudly (the library-level
    gate is a zero-statement no-op; the CLI does not let a mistyped invocation
    look like a completed disable). A crashed run converges on the re-run.
    """
    settings = TaskQSettings.load()
    if settings.timescaledb_hypertables:
        typer.echo(
            "TASKQ_TIMESCALEDB_HYPERTABLES is still true: disabling now would be a "
            "zero-statement no-op. Flip the flag off (and roll the workers, so they "
            "stop expecting the hypertable schema) before running this.",
            err=True,
        )
        raise typer.Exit(code=1)
    conn_factory = _credential_conn_factory(
        str(settings.pg_dsn),
        _resolved_ref(pg_credential_provider, settings.pg_credential_provider),
        option="--pg-credential-provider",
    )
    asyncio.run(_disable_hypertables(settings, conn_factory=conn_factory))


async def _disable_hypertables(
    settings: TaskQSettings,
    *,
    conn_factory: ConnFactory | None = None,
) -> None:
    """The ``migrate disable-hypertables`` body: the SAME advisory-lock and
    settings flow as ``migrate up`` (the deploy step's conversion and this
    disable serialize on the same migration advisory lock; WorkerSettings
    re-reads the same cascade the workers run with), with ``migrate up``'s
    loud-error exit discipline."""
    conn: asyncpg.Connection | None = None
    try:
        conn = await _open_migrate_conn(settings, conn_factory)
        async with migrate_mod.migration_advisory_lock(conn, schema=settings.schema_name):
            report = await disable_hypertables(
                conn, schema=settings.schema_name, settings=WorkerSettings.load()
            )
    except TimescaleDBUnavailableError as exc:
        # A capability refusal, not a schema failure: the error already
        # names the setting and what the server is missing.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    except SystemExit as exc:
        # Lock contention. Already a precise message.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        # The same self-diagnosing report migrate up prints: the schema
        # state on the still-open connection, never a raw traceback.
        await _report_up_failure(conn, settings.schema_name, exc)
        raise typer.Exit(code=1) from None
    finally:
        if conn is not None:
            await close_conn_bounded(conn, "migrate-disable-hypertables", CLOSE_TIMEOUT_SECS)
    if not report.converted:
        typer.echo("no hypertables to disable: the retention tables are already plain")
        return
    typer.echo(f"disabled hypertables on {len(report.converted)} table(s):")
    for table in report.converted:
        typer.echo(f"  restored to plain: {table}")
    for entry in report.retention_policies:
        typer.echo(f"  removed retention policy: {entry}")
    for table in report.compression_policies:
        typer.echo(f"  removed compression policy: {table}")


def _print_actor_config_row(row: ActorConfigRow) -> None:
    typer.echo(
        f"  {row.actor}: max_concurrent={row.max_concurrent} "
        f"max_pending={row.max_pending} queue={row.queue} "
        f"result_ttl={row.result_ttl} updated_at={row.updated_at}"
    )


@actor_config_app.command("list")
def actor_config_list() -> None:
    """List every stored actor_config row."""
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_list(settings))


async def _actor_config_list(settings: TaskQSettings) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await list_actor_configs(conn, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "actor-config-list", CLOSE_TIMEOUT_SECS)
    if not rows:
        typer.echo("no actor_config rows")
        return
    for row in rows:
        _print_actor_config_row(row)


@actor_config_app.command("get")
def actor_config_get(
    actor: Annotated[str, typer.Argument(help="Actor name.")],
) -> None:
    """Show the stored actor_config row for one actor."""
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_get(settings, actor))


async def _actor_config_get(settings: TaskQSettings, actor: str) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        row = await get_actor_config(conn, actor, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "actor-config-get", CLOSE_TIMEOUT_SECS)
    if row is None:
        typer.echo(f"no stored actor_config row for actor {actor!r}", err=True)
        raise typer.Exit(code=1)
    _print_actor_config_row(row)


@actor_config_app.command("set")
def actor_config_set(
    actor: Annotated[str, typer.Argument(help="Actor name.")],
    max_concurrent: Annotated[
        int | None,
        typer.Option(
            "--max-concurrent",
            min=0,
            help="New fleet-wide concurrency cap. Takes effect on the next dispatch cycle "
            "(no worker restart), the dispatch query re-reads this column every cycle.",
        ),
    ] = None,
    clear_max_concurrent: Annotated[
        bool, typer.Option("--clear-max-concurrent", help="Set max_concurrent back to unlimited.")
    ] = False,
    max_pending: Annotated[
        int | None,
        typer.Option(
            "--max-pending",
            min=0,
            help="New queue-depth backpressure cap. Takes effect within seconds on every "
            "enqueue-side process (bounded by each client's capacity-cache TTL, default 5s) "
            ", no redeploy, no worker restart.",
        ),
    ] = None,
    clear_max_pending: Annotated[
        bool,
        typer.Option(
            "--clear-max-pending",
            help="Clear the stored override; enforcement reverts to the @actor(max_pending=...) literal.",
        ),
    ] = False,
    result_ttl: Annotated[
        float | None,
        typer.Option(
            "--result-ttl",
            min=0,
            help="New result TTL in seconds. Takes effect for jobs completing after this "
            "change (no worker restart), the terminal-write UPDATE re-reads this column "
            "for every job.",
        ),
    ] = None,
    clear_result_ttl: Annotated[
        bool, typer.Option("--clear-result-ttl", help="Set result_ttl back to unset.")
    ] = False,
) -> None:
    """Update capacity fields on an existing actor_config row.

    Only flags actually passed are changed. An actor must already have a
    stored row (created by a worker startup that registered it) before
    its capacity can be tuned here.

    All three fields are live: ``--max-concurrent`` is re-read by the
    dispatch query every cycle and ``--result-ttl`` by the terminal-write
    path on every job completion (both immediate); ``--max-pending`` is
    re-read by every enqueue-side process through a TTL-bounded cache
    (default 5s staleness). No redeploy and no worker restart for any of
    them. Use ``taskq actor-config diff`` to see the stored value, the
    code literal, and which one the engine currently enforces.
    """
    if max_concurrent is not None and clear_max_concurrent:
        typer.echo("--max-concurrent and --clear-max-concurrent are mutually exclusive", err=True)
        raise typer.Exit(code=1)
    if max_pending is not None and clear_max_pending:
        typer.echo("--max-pending and --clear-max-pending are mutually exclusive", err=True)
        raise typer.Exit(code=1)
    if result_ttl is not None and clear_result_ttl:
        typer.echo("--result-ttl and --clear-result-ttl are mutually exclusive", err=True)
        raise typer.Exit(code=1)

    mc: int | Unset | None = UNSET
    if clear_max_concurrent:
        mc = None
    elif max_concurrent is not None:
        mc = max_concurrent

    mp: int | Unset | None = UNSET
    if clear_max_pending:
        mp = None
    elif max_pending is not None:
        mp = max_pending

    rt: float | Unset | None = UNSET
    if clear_result_ttl:
        rt = None
    elif result_ttl is not None:
        rt = result_ttl

    if isinstance(mc, Unset) and isinstance(mp, Unset) and isinstance(rt, Unset):
        typer.echo(
            "nothing to change, pass at least one --max-concurrent/--max-pending/--result-ttl "
            "or --clear-* flag",
            err=True,
        )
        raise typer.Exit(code=1)

    settings = TaskQSettings.load()
    asyncio.run(_actor_config_set(settings, actor, mc, mp, rt))


async def _actor_config_set(
    settings: TaskQSettings,
    actor: str,
    max_concurrent: int | Unset | None,
    max_pending: int | Unset | None,
    result_ttl: float | Unset | None,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        row = await set_actor_config_capacity(
            conn,
            actor,
            max_concurrent=max_concurrent,
            max_pending=max_pending,
            result_ttl=result_ttl,
            schema=settings.schema_name,
        )
    except ValueError as exc:
        # Validation failures (negative, NaN/±inf, bool) are operator
        # errors, print the reason, not a traceback.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    finally:
        await close_conn_bounded(conn, "actor-config-set", CLOSE_TIMEOUT_SECS)
    if row is None:
        typer.echo(
            f"no stored actor_config row for actor {actor!r}, it must be registered by a "
            "worker startup first",
            err=True,
        )
        raise typer.Exit(code=1)
    _print_actor_config_row(row)


@actor_config_app.command("deregister")
def actor_config_deregister(
    actor: Annotated[str, typer.Argument(help="Actor name to deregister.")],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Cancel pending/scheduled jobs and disable enabled cron schedules"
            " instead of refusing. Running jobs still block deregistration.",
        ),
    ] = False,
    purge_queue: Annotated[
        bool,
        typer.Option(
            "--purge-queue",
            help="Also delete the orphaned queues row if no other actor_config"
            " references the same queue.",
        ),
    ] = False,
) -> None:
    """Deregister an actor: delete its actor_config row with safety checks.

    By default refuses if non-terminal jobs or enabled cron schedules
    reference the actor. Use --force to cancel pending/scheduled jobs and
    disable schedules. Running jobs always block (force or not). Use
    --purge-queue to also delete the queues row if no other actor uses it.

    Exit codes: 0 success, 2 refusal (active jobs/schedules or invalid
    schema), 3 not found.
    """
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_deregister(settings, actor, force, purge_queue))


async def _actor_config_deregister(
    settings: TaskQSettings,
    actor: str,
    force: bool,
    purge_queue: bool,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        result = await deregister_actor(
            conn,
            actor,
            force=force,
            purge_queue=purge_queue,
            schema=settings.schema_name,
        )
    except ActorNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=3) from None
    except (ActorDeregistrationError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from None
    finally:
        await close_conn_bounded(conn, "actor-config-deregister", CLOSE_TIMEOUT_SECS)

    typer.echo(
        f"Deregistered actor {result.actor!r}:"
        f" actor_config_deleted={result.actor_config_deleted}"
        f" queue={result.queue!r}"
        f" schedules_disabled={result.schedules_disabled}"
        f" jobs_cancelled={result.jobs_cancelled}"
        f" terminal_jobs_remaining={result.terminal_jobs_remaining}"
        f" queue_purged={result.queue_purged}"
    )
    typer.echo(
        f"WARNING: Actor {result.actor!r} is now unregistered. Any future enqueue()"
        f" to this actor name will create a stranded pending job that will never"
        f" be dispatched. Stop enqueuing before deregistering.",
        err=True,
    )


@actor_config_app.command("move-queue")
def actor_config_move_queue(
    actor: Annotated[str, typer.Argument(help="Actor name to move.")],
    new_queue: Annotated[str, typer.Argument(help="Target queue name.")],
) -> None:
    """Move an actor to a different queue in ONE operator action.

    Rewrites the stored queue assignment, carries the old queue's mode and
    max_concurrent to the target when the target has no row of its own, and
    moves the actor's pending/scheduled backlog onto the target (bounded
    batches, then one final transaction for the flip) so old-queue strays
    drain through the target's consumers. Running jobs finish on the
    workers that claimed them; any that re-pend instead (failure retry,
    crash reclaim, operator retry) keep their old queue label as an audit
    trail but are ROUTED at dispatch by the actor's current assignment ,
    the tail drains through the target queue's consumers, never stranded
    on the retired source queue. Cron fires follow the moved assignment
    from the flip on.

    Workers boot on either side of the matching code deploy, in any order:
    a stale `@actor(queue=...)` literal logs `actor-config-queue-override`
    and adopts the stored assignment instead of refusing boot.

    Exit codes: 0 moved, 2 refusal (invalid queue name, the actor is
    already on that queue, or the assignment changed concurrently),
    3 no stored row.
    """
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_move_queue(settings, actor, new_queue))


async def _actor_config_move_queue(
    settings: TaskQSettings,
    actor: str,
    new_queue: str,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        result = await move_actor_queue(
            conn,
            actor,
            new_queue,
            schema=settings.schema_name,
        )
    except ActorNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=3) from None
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from None
    except DEADLINE_ERRORS:
        # A drain batch ran out of its deadline, server-side
        # (statement_timeout) or client-side (dropped connection, cancelled
        # await). Both are the same fact to an operator: this batch did not
        # land. Catching the named family rather than spelling the pair here
        # is what keeps the client half from escaping as an untyped error.
        # The batches that
        # committed before it are real progress and the drain's queue
        # predicate skips rows already moved, so the only action is to run
        # the command again, which is a refusal to report, not a crash.
        # The error text is not echoed: the server appends DETAIL quoting
        # row values, which must not cross this boundary.
        typer.echo(
            f"move-queue of actor {actor!r} onto {new_queue!r} aborted: a drain batch "
            "exceeded its statement timeout. Batches committed before the abort are "
            "kept, so the move is incomplete and safe to re-run, re-run the same "
            "command to continue from where it stopped.",
            err=True,
        )
        raise typer.Exit(code=2) from None
    finally:
        await close_conn_bounded(conn, "actor-config-move-queue", CLOSE_TIMEOUT_SECS)

    _report_queue_move(result)


@queue_app.command("migrate")
def queue_migrate(
    actor: Annotated[str, typer.Argument(help="Actor name to move.")],
    to: Annotated[
        str,
        typer.Option(
            "--to",
            help="Target queue name. Required, the target is never defaulted.",
        ),
    ],
) -> None:
    """Move an actor onto a different queue, and report the residual.

    Applies the coordinated writes of a move, the stored assignment and
    the target `queues` row carrying the source's mode and cap, in one
    transaction, so a failure leaves the deployment exactly where it
    started rather than half-moved. The actor's pending/scheduled backlog
    is rewritten onto the target first, as bounded committed batches.

    The target is named by `--to` rather than positionally: the two
    arguments of a move are an actor and a queue, both plain strings, and
    two bare positionals are the shape an operator transposes under
    pressure, with the consequence that the backlog drains onto a queue
    that was never the target.

    Reports how many pending jobs still carry the source queue label.
    Producers still running the old literal keep placing jobs there, and
    those stay served by the source queue's consumers, so that count is
    what tells the operator when the retired queue can stop being consumed.

    Exit codes: 0 moved, 2 refusal (invalid queue name, the actor is
    already on that queue, the assignment changed concurrently, or a drain
    batch timed out and the move is re-runnable), 3 no stored row.
    """
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_move_queue(settings, actor, to))


def _report_queue_move(result: ActorQueueMoveResult) -> None:
    """Print one completed move, and its residual to stderr.

    The residual goes to stderr because it drives the operator's next
    action while it is non-zero (keep the source queue's consumers up
    until it drains), not part of the move's result record, the move
    itself succeeded.
    """
    typer.echo(
        f"Moved actor {result.actor!r}: {result.from_queue!r} -> {result.to_queue!r}"
        f" jobs_moved={result.jobs_moved}"
        f" running_jobs_left={result.running_jobs_left}"
        f" queues_row_carried={result.queues_row_carried}"
    )
    residual = result.pending_jobs_on_old_queue
    residual_line = (
        f"{residual} pending/scheduled job(s) still carry queue {result.from_queue!r} "
        "(placed there by producers still running the old literal)."
    )
    if residual > 0:
        # The operator's next action is only advice while there is a
        # residual to drain, at zero the condition is already met and
        # repeating it reads as an outstanding action.
        residual_line += (
            " Keep that queue's consumers running until every producer carries "
            "the new literal and this count reaches zero."
        )
    typer.echo(residual_line, err=True)
    typer.echo(
        f"NOTE: ensure workers consume {result.to_queue!r} now, and keep "
        f"consuming {result.from_queue!r} until every producer runs the "
        f"matching literal, stale producers keep enqueueing to "
        f"{result.from_queue!r}, and those strays stay served by "
        f"{result.from_queue!r}'s consumers. Left-behind running jobs "
        f"(running_jobs_left={result.running_jobs_left}) finish on their "
        f"claiming workers; any that re-pend route to {result.to_queue!r}'s "
        f"consumers via the assignment, so they drain even after "
        f"{result.from_queue!r} is retired.",
        err=True,
    )


_CAPACITY_DIFF_FIELDS = ("max_concurrent", "max_pending", "result_ttl")


def _literal_for_field(ref: ActorRef[Any, Any], field: str) -> object:
    value: object = getattr(ref, field)
    if field == "result_ttl" and value is not None:
        # The stored column is float seconds; show the literal in the same unit.
        return cast("timedelta", value).total_seconds()
    return value


def _effective_capacity(field: str, literal: object, row: ActorConfigRow) -> tuple[object, str]:
    """The value the engine enforces for one capacity field, and its source.

    Mirrors the enforcement semantics field by field: ``max_concurrent``
    is read by the dispatch SQL, which cannot see the code literal, once
    a row exists, the stored column is fully authoritative and NULL means
    unlimited. ``max_pending`` / ``result_ttl`` fall back to the literal
    when the stored value is NULL (clearing reverts to the code default).
    """
    stored = getattr(row, field)
    if field == "max_concurrent":
        return ("unlimited" if stored is None else stored, "stored")
    if stored is not None:
        return (stored, "stored")
    return (literal, "literal")


def _print_actor_diff(
    name: str, ref: ActorRef[Any, Any] | None, row: ActorConfigRow | None
) -> bool:
    """Print one actor's diff; return whether its state fails the gate.

    Three states fail: a queue mismatch (assignment drift, boot adopts the
    stored queue, but the cron leader's fires follow it while producers
    enqueue by their own literal, so the two routing halves disagree until
    the move or the deploy completes), a metadata mismatch (the next worker
    startup raises ActorConfigDriftList), and a registry actor with no
    stored row (the dispatch capacity gate reads only actor_config rows, so
    the actor does not dispatch until a row is seeded). Capacity-only
    differences never fail, stored capacity is operator-owned by design ,
    and neither does a leftover row for an actor that is no longer
    registered: it only serves already-queued jobs.
    """
    typer.echo(f"{name}:")
    if row is None:
        # Registry-only actor: nothing has ever seeded a row. Enforcement
        # differs per field: the dispatch CTE builds its candidate gate
        # FROM actor_config (inner join), so with no row the actor is
        # NEVER dispatched, max_concurrent is effectively 0, not the
        # literal. max_pending / result_ttl enforcement can see the code
        # literal, so those fall back to it.
        assert ref is not None  # row is None only when the name came from the registry
        typer.echo(
            "  no stored row, never synced; the row is seeded at the next "
            "worker startup. Until then the actor DOES NOT DISPATCH (the "
            "dispatch capacity gate reads only actor_config rows)."
        )
        for field in _CAPACITY_DIFF_FIELDS:
            literal = _literal_for_field(ref, field)
            if field == "max_concurrent":
                typer.echo(
                    f"  {field:<15} literal={literal}  effective=0 (no stored row, "
                    "actor cannot dispatch)"
                )
            else:
                typer.echo(f"  {field:<15} literal={literal}  effective={literal} (literal)")
        typer.echo(f"  {'queue':<15} literal={ref.queue}")
        return True
    if ref is None:
        typer.echo(
            "  stored row's actor is not in the registry, leftover row; "
            "only already-queued jobs can still reference it"
        )
        _print_actor_config_row(row)
        return False
    for field in _CAPACITY_DIFF_FIELDS:
        literal = _literal_for_field(ref, field)
        stored = getattr(row, field)
        effective, source = _effective_capacity(field, literal, row)
        typer.echo(
            f"  {field:<15} literal={literal}  stored={stored}  effective={effective} ({source})"
        )
    queue_mismatch = ref.queue != row.queue
    if queue_mismatch:
        typer.echo(
            f"  {'queue':<15} literal={ref.queue}  stored={row.queue}  MISMATCH, assignment "
            "drift: boot adopts the stored queue (actor-config-queue-override) and "
            "cron fires follow it while producers enqueue by their own literal. "
            "Reconcile with `taskq actor-config move-queue ACTOR NEW_QUEUE` or "
            "deploy the matching literal."
        )
    else:
        typer.echo(f"  {'queue':<15} {row.queue} (match)")
    metadata_mismatch = dict(ref.metadata) != row.metadata
    if metadata_mismatch:
        typer.echo(
            f"  {'metadata':<15} literal={dict(ref.metadata)}  stored={row.metadata}  MISMATCH, "
            "structural drift; the next worker startup raises ActorConfigDriftList unless run "
            "with --force-update-actor-config"
        )
    else:
        typer.echo(f"  {'metadata':<15} (match)")
    return queue_mismatch or metadata_mismatch


@actor_config_app.command("diff")
def actor_config_diff(
    actors: Annotated[
        str,
        typer.Option(
            "--actors",
            help="Module:attr reference to the actor registry (e.g. myapp.actors:registry). "
            "Stored rows are compared against these code literals.",
        ),
    ],
) -> None:
    """Diff stored actor_config rows against the code literals in a registry.

    Per actor and field, shows the @actor(...) literal, the stored value,
    and the value the engine actually enforces right now ("effective").
    Reach for this when debugging "why is my change not taking effect":
    a capacity literal that differs from the stored row is IGNORED at
    runtime, the stored value wins; tune it with `taskq actor-config
    set`, and a queue mismatch means the two routing halves disagree
    (boot adopts the stored queue and cron fires follow it while producers
    enqueue by their own literal); reconcile with `taskq actor-config
    move-queue`. A metadata mismatch still refuses the next worker
    startup with ActorConfigDriftList.

    Exit codes: 0 no gate-failing drift; 1 at least one actor fails the
    gate, a registry actor with no stored row (it does not dispatch until
    one is seeded), a queue assignment mismatch (producer literals and
    cron routing disagree), or a metadata mismatch (startup-blocking).
    Capacity-only differences never affect the exit code: stored capacity
    is operator-owned by design, so they are reportable drift, not
    gate-failing drift.
    """
    registry = _load_actor_registry(actors)
    settings = TaskQSettings.load()
    asyncio.run(_actor_config_diff(settings, registry))


async def _actor_config_diff(
    settings: TaskQSettings,
    registry: Mapping[str, ActorRef[Any, Any]],
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await list_actor_configs(conn, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "actor-config-diff", CLOSE_TIMEOUT_SECS)
    stored_by_actor = {row.actor: row for row in rows}

    names = sorted(set(registry) | set(stored_by_actor))
    if not names:
        typer.echo("no actors in the registry and no stored actor_config rows")
        return
    blocking = False
    for name in names:
        if _print_actor_diff(name, registry.get(name), stored_by_actor.get(name)):
            blocking = True
    # The whole report prints before this exit: the output is the diagnosis,
    # the exit code is the signal a CI gate checks.
    if blocking:
        raise typer.Exit(code=1)


def _describe_actor_capacity(row: ActorConfigRow) -> str:
    """One actor's stored capacity, with every special value named.

    A bare ``0`` and a bare blank are the two values an operator most
    often misreads: zero looks like a broken row rather than the drain it
    is, and NULL looks like half-written data rather than the "no
    actor-level cap" it actually configures. Both get a word.
    """
    if row.max_concurrent is None:
        concurrent = "uncapped (stored NULL, no actor-level cap)"
    elif row.max_concurrent == 0:
        concurrent = "0, DRAIN MODE (deliberately stopped; jobs enqueue and never run)"
    else:
        concurrent = str(row.max_concurrent)
    pending = "unlimited (stored NULL)" if row.max_pending is None else str(row.max_pending)
    return f"max_concurrent={concurrent}  max_pending={pending}"


async def _list_stranded_pending_jobs(
    conn: asyncpg.Connection,
    *,
    schema: str,
    worker_liveness_seconds: int,
) -> list[_StrandedActorJobs]:
    """Pending/scheduled jobs grouped by the actor nothing alive consumes.

    The same computation the leader's stranded-jobs sweep runs every
    minute, issued here on demand: ``doctor`` is the surface an operator
    reaches for mid-incident, and it cannot wait on a leader tick.  The
    routing-queue discriminator (a re-pended row routes by its actor's
    stored assignment, not its label) is dispatch's own contract, mirrored
    from the sweep so both surfaces answer the same question the same way.
    "Serves" means a LIVE worker subscribes the queue, the sweep's own
    liveness arm: a worker row whose heartbeat has gone stale must not
    count as serving until the stale-worker sweep removes it, or a ghost
    row hides an unserved queue from the operator mid-incident. The
    result is per ACTOR, bounded by the distinct-actor count, never by
    backlog depth.
    """
    # Defence in depth: TaskQSettings validates schema_name at load;
    # re-check at the SQL interpolation site (the queue_ops convention).
    require_schema(schema)
    rows = await conn.fetch(
        f"""\
SELECT s.actor,
       count(*) FILTER (WHERE s.no_actor_config)::int AS no_actor_config_cnt,
       count(*) FILTER (WHERE s.unserved_queue)::int AS unserved_queue_cnt,
       coalesce(
         array_agg(DISTINCT s.routing_queue) FILTER (WHERE s.unserved_queue),
         ARRAY[]::text[]
       ) AS unserved_queues
FROM (
    SELECT r.actor,
           r.routing_queue,
           r.no_actor_config,
           NOT r.no_actor_config
             AND NOT EXISTS (
               SELECT 1 FROM "{schema}".workers w
               WHERE r.routing_queue = ANY(w.queues)
                 -- Mirrors the leader sweep's liveness arm: a worker row
                 -- whose heartbeat has gone stale is not dispatching; until
                 -- the stale-worker sweep removes it, it must not count as
                 -- serving the queue. statement_timestamp() (STABLE), the
                 -- two-clock rule every sampler follows; the window is the
                 -- admin UI's own liveness setting.
                 AND w.last_seen_at > statement_timestamp()
                      - make_interval(secs => $1)
             ) AS unserved_queue
    FROM (
        SELECT j.actor,
               CASE WHEN j.assignment_routed THEN ac.queue ELSE j.queue END
                 AS routing_queue,
               NOT EXISTS (
                 SELECT 1 FROM "{schema}".actor_config ac2 WHERE ac2.actor = j.actor
               ) AS no_actor_config
        FROM "{schema}".jobs j
        LEFT JOIN "{schema}".actor_config ac ON ac.actor = j.actor
        WHERE j.status IN ('pending', 'scheduled')
    ) r
) s
WHERE s.no_actor_config OR s.unserved_queue
GROUP BY s.actor""",  # noqa: S608  # Why: schema is identifier-validated above and double-quoted; no user values are interpolated.
        worker_liveness_seconds,
    )
    return [
        _StrandedActorJobs(
            actor=str(r["actor"]),
            no_actor_config=int(r["no_actor_config_cnt"]),
            unserved_queue=int(r["unserved_queue_cnt"]),
            unserved_queues=tuple(str(q) for q in r["unserved_queues"]),
        )
        for r in rows
    ]


async def _list_worker_stall_tallies(
    conn: asyncpg.Connection,
    *,
    schema: str,
) -> list[tuple[str, dict[str, object]]]:
    """Read each live worker's attributed-stall tally from its row metadata.

    The heartbeat merges the tally (``loop_stalls``: actor -> kind ->
    count) into the metadata the worker registered with; a worker that
    has attributed nothing carries no key and contributes nothing here.
    Malformed metadata (a non-dict tally, or non-dict kind counts) is
    skipped rather than raised: a hand-edited or stale row must not stop
    the whole report. Read-only, like every doctor read.
    """
    require_schema(schema)
    rows = await conn.fetch(
        f'SELECT id, metadata FROM "{schema}".workers'  # noqa: S608  # Why: schema is identifier-validated above and double-quoted; no user values are interpolated.
    )
    tallies: list[tuple[str, dict[str, object]]] = []
    for row in rows:
        metadata: object = row["metadata"]
        if isinstance(metadata, str):
            # A plain asyncpg.connect returns jsonb as text: the doctor's
            # reporting connection registers no codec, so the heartbeat's
            # tally arrives as a JSON string here (measured: the dict
            # check alone silently dropped EVERY live tally).
            try:
                # orjson raises JSONDecodeError, a ValueError subclass, so
                # the except below catches it unchanged. via _json per the
                # library's orjson-only doctrine (taskq/_json.py).
                metadata = json_loads(metadata)
            except ValueError:
                continue
        if not isinstance(metadata, dict):
            continue
        metadata_map = cast("dict[str, object]", metadata)
        tally = metadata_map.get("loop_stalls")
        if isinstance(tally, dict) and tally:
            tallies.append((str(row["id"]), cast("dict[str, object]", tally)))
    return tallies


@app.command("doctor")
def doctor(
    actors: Annotated[
        str,
        typer.Option(
            "--actors",
            help="Module:attr reference to the actor registry (e.g. myapp.actors:registry). "
            "Stored rows are read against these registered actors.",
        ),
    ],
    platform_grace_seconds: Annotated[
        float | None,
        typer.Option(
            "--platform-grace-seconds",
            help="The orchestrator's stop grace for this deployment (Kubernetes "
            "terminationGracePeriodSeconds, ACA/ECS stop timeout, compose "
            "stop_grace_period, systemd TimeoutStopSec). When given, doctor compares "
            "it against the worker's modelled worst-case shutdown and reports the "
            "shortfall: a platform grace below it SIGKILLs the worker mid-teardown "
            "and in-flight work re-runs.",
        ),
    ] = None,
) -> None:
    """Report capacity and configuration conditions that fail silently.

    TaskQ refuses boot only on structural stored-config drift, so a whole
    family of misconfigurations produces no error at all: an actor with no
    stored row never dispatches, a leftover `queues` row caps an actor
    nobody thinks is capped, a stored `max_concurrent=0` drains an
    actor that looks configured, and a job already pending for an actor
    nothing consumes waits forever. Each one's only symptom is work that
    does not happen. This is the one command that names them together.

    The report leads with the DETECTED storage mode (vanilla,
    timescale-apache, or timescale-tsl - detected from the server, not
    read from settings) and that mode's capability consequences in one
    glance.

    Read-only: it issues no writing statement, so it is safe to run
    against production mid-incident.

    Exit code: 0 when the report rendered (every condition reported here
    is one a worker keeps running through; a diagnostic that fails the
    shell gets wrapped in `|| true` and then ignored). A connection
    failure prints `doctor failed: <cause>` plus an Action line to
    stderr and exits 1, migrate up's failure pattern. Gating CI on
    drift is `taskq actor-config diff`, which exits non-zero by design.
    """
    registry = _load_actor_registry(actors)
    settings = WorkerSettings.load()
    try:
        asyncio.run(_doctor(settings, registry, platform_grace_seconds))
    except (SystemExit, typer.Exit):
        raise
    except Exception as exc:
        # An unreachable Postgres used to escape as a raw traceback (the
        # connect's OSError, exit 1), a shape no other CLI failure surface
        # shows. The report's own contract is a rendered answer, so the
        # connection failure gets the same treatment every other CLI
        # failure gets: migrate up's two-line pattern, the cause's
        # headline and the one action to take, no traceback.
        typer.echo(f"doctor failed: {migrate_mod._exception_headline(exc)}", err=True)  # pyright: ignore[reportPrivateUsage]  # Why: the headline rule (first line, else type name) is migrate's own; sharing the helper keeps the two surfaces from drifting.
        typer.echo("Action: fix the error and re-run `taskq doctor`.", err=True)
        raise typer.Exit(code=1) from None


async def _doctor(
    settings: WorkerSettings,
    registry: Mapping[str, ActorRef[Any, Any]],
    platform_grace_seconds: float | None = None,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await list_actor_configs(conn, schema=settings.schema_name)
        queues = await list_queues(conn, schema=settings.schema_name)
        stranded = await _list_stranded_pending_jobs(
            conn,
            schema=settings.schema_name,
            worker_liveness_seconds=settings.admin_worker_liveness_seconds,
        )
        worker_stalls = await _list_worker_stall_tallies(conn, schema=settings.schema_name)
        # The insight families over the doctor's 24h window (the
        # derivation sits on _DOCTOR_INSIGHTS_WINDOW): every one a
        # pure-read statement from taskq/insights.py, so the read-only
        # contract — no writing statement anywhere in this command —
        # holds across the new families too.
        imbalance_rows = await fetch_queue_imbalance(conn, schema=settings.schema_name)
        wait_rows = await fetch_wait_distribution(
            conn, schema=settings.schema_name, window=_DOCTOR_INSIGHTS_WINDOW
        )
        overprovisioning_rows = await fetch_overprovisioning(
            conn, schema=settings.schema_name, window=_DOCTOR_INSIGHTS_WINDOW
        )
        drain_rows = await fetch_drain_estimates(
            conn, schema=settings.schema_name, window=_DOCTOR_INSIGHTS_WINDOW
        )
        cron_rows = await fetch_cron_ledger(
            conn, schema=settings.schema_name, window=_DOCTOR_INSIGHTS_WINDOW
        )
        # The detected storage mode, read on the report's own connection
        # while it is open: the first finding family renders from it. On a
        # server detected apache the same connection also probes for
        # conversion-era policy jobs a license downgrade strands (the
        # healthy apache mode registers none, so any answer is debris).
        storage_mode = await detect_storage_mode(conn)
        downgraded_policies = (
            await probe_registered_policy_jobs(conn, schema=settings.schema_name)
            if storage_mode is StorageMode.TIMESCALE_APACHE
            else ()
        )
    finally:
        await close_conn_bounded(conn, "doctor", CLOSE_TIMEOUT_SECS)

    typer.echo("stored actor capacity:")
    for row in sorted(rows, key=lambda r: r.actor):
        typer.echo(f"  {row.actor:<20} queue={row.queue:<16} {_describe_actor_capacity(row)}")
    if not rows:
        typer.echo("  (no stored actor_config rows)")

    # Scanned from the process environment the operator ran doctor in: the
    # settings load itself cannot see a name it does not define, which is
    # exactly why the scan lives here (see _unknown_taskq_env_vars).
    findings = _doctor_findings(
        registry,
        rows,
        queues,
        stranded,
        worker_stalls,
        unknown_env_vars=_unknown_taskq_env_vars(),
        storage_mode=storage_mode,
        timescaledb_flag=settings.timescaledb_hypertables,
        downgraded_policies=downgraded_policies,
        imbalance_rows=imbalance_rows,
        wait_rows=wait_rows,
        overprovisioning_rows=overprovisioning_rows,
        drain_rows=drain_rows,
        cron_rows=cron_rows,
        insights_window=_DOCTOR_INSIGHTS_WINDOW,
    )

    # The one finding that needs an operator-supplied number: the platform's
    # stop grace (Kubernetes terminationGracePeriodSeconds, ACA/ECS stop
    # timeout, compose stop_grace_period, systemd TimeoutStopSec) is
    # invisible to the running worker, and when it sits below the worker's
    # own modelled worst case the orchestrator SIGKILLs the teardown
    # mid-flight, the shutdown degrades to crash reclaim (leases expire,
    # work re-runs) exactly when a deploy is already in progress. The
    # comparison runs here, where the worst case is computed from the same
    # settings the fleet boots with.
    worst_case = settings.worst_case_shutdown_seconds
    if platform_grace_seconds is not None:
        if platform_grace_seconds < worst_case:
            findings.append(
                f"platform stop grace ({platform_grace_seconds:g}s) is below the worker's "
                f"modelled worst-case shutdown ({worst_case:.0f}s): the orchestrator "
                "SIGKILLs the worker mid-teardown and shutdown degrades to crash reclaim "
                "(leases expire, in-flight work re-runs). Raise the platform grace above "
                "the worst case, or lower the shutdown budgets the worst case is computed "
                "from (docs/guides/upgrading.md carries the arithmetic)."
            )
        else:
            typer.echo(
                f"platform stop grace: {platform_grace_seconds:g}s covers the worker's "
                f"modelled worst-case shutdown ({worst_case:.0f}s)."
            )

    typer.echo("")
    if not findings:
        typer.echo("no findings, every registered actor has a stored row and every")
        typer.echo("queue row backs a live assignment.")
        return
    typer.echo(f"findings ({len(findings)}):")
    for finding in findings:
        typer.echo(f"  - {finding}")


async def _report_up_failure(conn: asyncpg.Connection | None, schema: str, exc: Exception) -> None:
    """Print a self-diagnosing ``migrate up`` failure report to stderr.

    TaskQ users must never inspect catalog state by hand, so this reports ,
    gathered on the still-open connection, what failed, what state the
    schema is in (INVALID indexes included), and the single action to take.
    The diagnosis lives in :mod:`taskq.migrate` (shared with the
    worker/startup path).

    The report must NEVER mask the original error: when the conn was never
    acquired (connect itself failed) or the diagnosis itself raises, the
    fallback is the generic two-line report, the original error's headline
    and the fix-and-re-run action.
    """
    diagnosis: migrate_mod.ApplyFailureDiagnosis | None = None
    if conn is not None:
        with contextlib.suppress(Exception):
            diagnosis = await migrate_mod.diagnose_apply_failure(conn, schema, exc)
    if diagnosis is None:
        diagnosis = migrate_mod.ApplyFailureDiagnosis(
            headline=migrate_mod._exception_headline(exc),  # pyright: ignore[reportPrivateUsage]  # Why: the headline rule (first line, else type name) must match diagnose_apply_failure exactly; sharing the helper keeps the two from drifting.
            failed_filename=None,
            use_transaction=None,
            invalid_indexes=(),
            schema=schema,
        )
    for line in migrate_mod.render_apply_failure_lines(diagnosis):
        typer.echo(line, err=True)


async def _health_request(settings: WorkerSettings, path: str) -> int:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(settings.health_socket_path),
            timeout=_CONNECT_TIMEOUT_S,
        )
    except (TimeoutError, FileNotFoundError, ConnectionRefusedError, OSError) as exc:
        typer.echo(f"health socket unreachable: {exc}", err=True)
        if (
            WorkerSettings.resolve_cascade_value("TASKQ_HEALTH_SOCKET_PATH") is None
            and settings.health_socket_path
            == WorkerSettings.get_fields()["health_socket_path"][1].default  # pyright: ignore[reportAttributeAccessIssue,reportIndexIssue]  # Why: the field's static fallback literal, read rather than restated so the two cannot drift (get_fields is the settings base's own reflection API; [1] is the FieldInfo).
        ):
            # The probe ran with NO explicit path anywhere in the cascade,
            # so it aimed at the static fallback that an unconfigured
            # worker no longer binds (it mints /tmp/taskq_health_<pid>.sock
            # at boot). The unreachable line alone would send the operator
            # hunting for a worker that is perfectly healthy; the hint is
            # the per-pid discovery contract's other half. An operator who
            # DID set an explicit path (here or in the cascade) gets the
            # plain line: their value was honored verbatim.
            typer.echo(
                "hint: with no explicit path an unconfigured worker binds a "
                "per-process default (/tmp/taskq_health_<pid>.sock — the path its "
                "boot logs as health-server-started's socket_path); point "
                "TASKQ_HEALTH_SOCKET_PATH at that file and re-run.",
                err=True,
            )
        return 1
    try:
        async with asyncio.timeout(_REQUEST_TIMEOUT_S):
            writer.write(b"GET %s HTTP/1.0\r\nHost: localhost\r\n\r\n" % path.encode("ascii"))
            await writer.drain()
            status_line = await reader.readline()
            while True:
                line = await reader.readline()
                if line == b"\r\n" or not line:
                    break
            body = await reader.read()
        parts = status_line.decode("ascii", errors="replace").split(" ", 2)
        status_code = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
        typer.echo(body.decode("utf-8"))
        return 0 if 200 <= status_code < 300 else 1
    except TimeoutError:
        typer.echo("health request timed out", err=True)
        return 1
    finally:
        writer.close()
        with contextlib.suppress(OSError, TimeoutError):
            await asyncio.wait_for(writer.wait_closed(), timeout=1.0)


@health_app.command("live")
def health_live() -> None:
    settings = WorkerSettings.load()
    with asyncio.Runner() as runner:
        code = runner.run(_health_request(settings, "/live"))
    raise typer.Exit(code=code)


@health_app.command("ready")
def health_ready() -> None:
    settings = WorkerSettings.load()
    with asyncio.Runner() as runner:
        code = runner.run(_health_request(settings, "/ready"))
    raise typer.Exit(code=code)


@health_app.command("metrics")
def health_metrics() -> None:
    settings = WorkerSettings.load()
    with asyncio.Runner() as runner:
        code = runner.run(_health_request(settings, "/metrics"))
    raise typer.Exit(code=code)


def _build_sso_bundle(settings: TaskQSettings, base_path: str) -> Any | None:
    """Build an SSO ``AuthBundle`` from settings, or ``None`` when SSO is disabled.

    Returns ``None`` when ``TASKQ_SSO_BACKEND=none`` (the default), preserving
    the existing unauthenticated/BYO-auth behavior.
    """
    backend = settings.sso_backend.lower()
    secure = not settings.is_dev_environment
    if backend == "oidc":
        from taskq.web.admin.auth import OIDCAuthConfig, create_oidc_auth

        oidc = settings.oidc
        config = OIDCAuthConfig(
            issuer=oidc.issuer,
            client_id=oidc.client_id,
            # Unwrap at the boundary: the settings layer keeps these as
            # SecretStr so a settings repr can never leak them; the runtime
            # auth config is the one place they must exist as plain strings.
            client_secret=oidc.client_secret.get_secret_value(),
            redirect_uri=oidc.redirect_uri,
            session_secret=oidc.session_secret.get_secret_value(),
            session_max_age_seconds=oidc.session_max_age_seconds,
            secure_cookie=secure,
            scope=oidc.scope,
            group_claim=oidc.group_claim,
            allowed_groups=oidc.allowed_groups_set,
        )
        return create_oidc_auth(config, base_path=base_path)
    if backend == "saml":
        from taskq.web.admin.auth import SAMLAuthConfig, create_saml_auth

        saml = settings.saml
        config = SAMLAuthConfig(
            entity_id=saml.entity_id,
            acs_url=saml.acs_url,
            idp_entity_id=saml.idp_entity_id,
            idp_sso_url=saml.idp_sso_url,
            idp_x509_cert=saml.idp_x509_cert,
            sp_x509_cert=saml.sp_x509_cert,
            # Unwrap at the boundary (same rationale as the OIDC branch).
            sp_private_key=(
                saml.sp_private_key.get_secret_value() if saml.sp_private_key is not None else None
            ),
            session_secret=saml.session_secret.get_secret_value(),
            # Must be threaded through explicitly, exactly as the OIDC branch above does:
            # SAMLAuthConfig carries its own 28800 default, so omitting this silently pinned
            # every SAML deployment to 8h and made TASKQ_SAML_SESSION_MAX_AGE_SECONDS, a
            # documented, `ge=60`-validated knob, a no-op. It bounds the itsdangerous
            # signature max_age (auth/_session.py), i.e. how long a stolen admin cookie stays
            # valid, so an operator shortening it must actually take effect.
            session_max_age_seconds=saml.session_max_age_seconds,
            secure_cookie=secure,
            group_attribute=saml.group_attribute,
            allowed_groups=saml.allowed_groups_set,
            # Threaded explicitly (same rationale as session_max_age_seconds):
            # SAMLAuthConfig carries its own default, so a missed pass-through
            # would silently pin the flag off and make
            # TASKQ_SAML_ALLOW_COOKIELESS_FALLBACK a no-op.
            allow_cookieless_fallback=saml.allow_cookieless_fallback,
        )
        return create_saml_auth(config, base_path=base_path)
    return None


async def _build_ui_pool(pool_factory: PoolFactory) -> asyncpg.Pool:
    """Invoke *pool_factory* under the UI's first-use bound.

    Why bounded: UI startup arms no watchdog, a hung token endpoint inside
    the factory would park `taskq ui serve` forever before any request is
    served, and a hung rotation would park the reload loop with the old
    pool still serving and nothing reporting it. _UI_FACTORY_TIMEOUT_SECS
    is the SAME bound the worker applies to its bootstrap and reload
    factory calls (worker/deps.py).
    """
    try:
        pool = await asyncio.wait_for(pool_factory(), timeout=_UI_FACTORY_TIMEOUT_SECS)
    except TimeoutError as exc:
        raise TimeoutError(
            f"taskq ui serve: pool_factory did not return within "
            f"{_UI_FACTORY_TIMEOUT_SECS}s, the credential "
            "provider behind it (e.g. a token endpoint) is "
            "black-holed. UI startup fails loudly instead of "
            "parking forever."
        ) from exc
    assert pool is not None, "pool_factory returned None"
    return pool


def _ui_live_pool(application: Any) -> asyncpg.Pool:
    """The admin pool currently serving requests.

    ``app.state.pg_pool`` is the single live reference: admin routes
    resolve it per request through ``get_pg_pool`` and a credential
    rotation replaces it there, so anything that runs across a rotation
    (the readiness probe, the shutdown close) reads it here instead of
    capturing the pool it started with.
    """
    pool: asyncpg.Pool = application.state.pg_pool
    return pool


@contextlib.asynccontextmanager
async def _ui_credential_rotation(
    application: Any, pool_factory: PoolFactory, settings: TaskQSettings
) -> AsyncGenerator[None]:
    """Rebuild the admin pool on SIGHUP and on its :class:`ReloadSchedule`.

    The UI's counterpart of the worker's reload coordinator. A provider
    that issues a username-bearing pair (Vault dynamic credentials) pins
    the pool to that pair for its life - the ``password=`` callable can
    refresh a token per connection but never a username - so without a
    rebuild every connection recycled after the lease expires fails
    authentication and the admin UI dies quietly one lease after deploy.
    The cadence is the operator's ``TASKQ_RELOAD_INTERVAL`` when set,
    otherwise derived from the lease the factory was granted (half the
    TTL, see :class:`taskq.auth.ReloadSchedule`); a factory whose schedule
    can derive nothing only rotates on SIGHUP, and has already warned.

    A rebuild builds the new pool first and swaps it into
    ``app.state.pg_pool`` only once it exists, so a failed factory call
    leaves the live pool serving; the old pool is then closed with the
    same bounded drain the shutdown uses. SIGHUP is registered on the
    running loop where the platform allows it (not Windows, not a
    non-main thread); elsewhere the schedule alone drives rotation and the
    absence is logged once.
    """
    from taskq._reload_loop import run_reload_schedule

    declared = reload_schedule_of(pool_factory)
    schedule = ReloadSchedule(
        configured=settings.reload_interval,
        sources=(declared,) if declared is not None else (),
    )
    trigger = asyncio.Event()

    async def _rebuild() -> None:
        new_pool = await _build_ui_pool(pool_factory)
        old_pool = _ui_live_pool(application)
        application.state.pg_pool = new_pool
        await close_pool_bounded(old_pool, "ui-admin-reload", CLOSE_TIMEOUT_SECS)

    loop = asyncio.get_running_loop()
    sighup_registered = False
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, trigger.set)
            sighup_registered = True
        except (NotImplementedError, RuntimeError, ValueError):
            # Not the main thread (a test client's portal) or a platform
            # without loop signal handlers: the schedule still rotates.
            logger.warning("sighup-handler-unavailable", role="ui-admin", os_name=os.name)
    logger.info(
        "ui-credential-rotation-armed",
        reload_interval=schedule.interval,
        derived_from_lease=schedule.derived,
        lease_duration=schedule.lease_duration,
        sighup=sighup_registered,
    )
    task = asyncio.create_task(
        run_reload_schedule(schedule, _rebuild, trigger=trigger, role="ui-admin"),
        name="ui.credential_rotation",
    )
    try:
        yield
    finally:
        if sighup_registered:
            loop.remove_signal_handler(signal.SIGHUP)
        task.cancel()
        await reap_cancelled_child(task)


def _ui_serve(
    pg_dsn: str,
    schema: str,
    redis_url: str | None,
    host: str,
    port: int,
    run_migrate: bool,
    settings: TaskQSettings,
    pool_factory: PoolFactory | None = None,
    conn_factory: ConnFactory | None = None,
    redis_factory: RedisFactory | None = None,
    providers: Sequence[object] = (),
) -> None:
    """Serve the admin UI.

    *providers* are the credential providers behind the factories - loaded
    by this process, so this process releases what they hold (an Entra ID
    provider's lazily created credential keeps an aiohttp session open)
    at lifespan exit, bounded like every other teardown close.
    """
    from contextlib import asynccontextmanager

    try:
        from fastapi import APIRouter, Depends, FastAPI, Response
        from fastapi.responses import RedirectResponse
    except ImportError as exc:
        raise ImportError(
            "the admin UI requires the [fastapi] extra. "
            "Install it with: pip install 'taskq-py[fastapi]'"
        ) from exc

    from taskq.web._routing import HeadForGetRoute
    from taskq.web.admin import create_router, setup_admin_state

    sso_bundle = _build_sso_bundle(settings, base_path="/admin")
    auth_dependency = sso_bundle.dependency if sso_bundle is not None else None

    health_deps: list[Any] = []
    # Unwrap for the set-check, None-safe: a SecretStr is ALWAYS truthy (even
    # the empty default), so `if settings.health_token:` would enable token
    # auth with an empty token; and an unset field loads as None, not "".
    if settings.health_token is not None and settings.health_token.get_secret_value():
        from taskq.web.admin.auth import token_auth

        health_deps = [Depends(token_auth(settings.health_token.get_secret_value()))]
    elif not settings.is_dev_environment:
        if settings.health_require_token:
            raise RuntimeError(
                "health/metrics endpoints require TASKQ_HEALTH_TOKEN in non-dev "
                "environments (set TASKQ_HEALTH_REQUIRE_TOKEN=false to disable)"
            )
        logger.warning("health-metrics-no-auth", environment=settings.environment)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        import time
        from contextlib import AsyncExitStack

        from taskq import _json
        from taskq.connections import statement_cache_kwargs
        from taskq.worker.health import (
            _check_live,  # pyright: ignore[reportPrivateUsage]  # Why: _check_live is a shared utility consumed by both transports (Unix socket + FastAPI); the underscore signals "internal to the health subsystem" not "private to health.py".
        )

        if run_migrate:
            # Pre phase only. This path fires on process lifecycle events
            # nobody sequences, a pod restart, a rollout, an autoscale
            # event, not on an operator's decision. Post-phase migrations
            # exist to be withheld until the whole fleet is confirmed
            # upgraded, so applying them here would let an unrelated
            # restart close a rolling-deploy overlap window mid-rollout.
            # They stay behind an explicit `taskq migrate up --phase post`.
            if conn_factory is not None:
                await migrate_mod.apply_pending_locked(
                    conn_factory=conn_factory, schema=schema, phase="pre"
                )
            else:
                await migrate_mod.apply_pending_locked(pg_dsn, schema=schema, phase="pre")

        async with AsyncExitStack() as stack:
            # Pushed first so they unwind last: the pool and Redis client
            # built through them close before the credential they used.
            for provider in providers:
                stack.push_async_callback(
                    close_provider_bounded, provider, "ui-admin", CLOSE_TIMEOUT_SECS
                )
            # A credential-provider pool passes password= as an async
            # callable, so every physical connection this long-lived UI
            # process opens re-authenticates with a fresh token; the DSN
            # path is unchanged.
            if pool_factory is not None:
                pg_pool = await _build_ui_pool(pool_factory)
            else:
                # settings (the TaskQSettings this UI was launched with) is
                # in scope, so the pair resolves through statement_cache_kwargs;
                # forwarded explicitly so pyright can trace types through
                # asyncpg.create_pool.
                stmt_kwargs = statement_cache_kwargs(settings)
                # Why command_timeout: every admin-page query runs on this
                # pool, the pool-level per-query bound that closes all of
                # the sweep's admin query sites at once (a black-holed PG
                # wedges each request forever without it). Mirrors
                # dispatcher_command_timeout's default, the per-query bound
                # on every other pool the repo builds.
                pg_pool = await asyncpg.create_pool(
                    pg_dsn,
                    min_size=1,
                    max_size=4,
                    command_timeout=_UI_POOL_COMMAND_TIMEOUT_SECS,
                    statement_cache_size=stmt_kwargs["statement_cache_size"],
                    max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
                    connection_class=guarded_connection_class(),
                )
            assert pg_pool is not None, "asyncpg.create_pool returned None"
            # application.state.pg_pool is the one live pool: every admin
            # route resolves it per request (get_pg_pool), so a credential
            # rotation swaps it there and nothing holds a stale reference.
            application.state.pg_pool = pg_pool

            async def _close_ui_pool() -> None:
                # Why module-global reads at call time: tests monkeypatch
                # close_pool_bounded / CLOSE_TIMEOUT_SECS as
                # observation and timeout-shrink seams (same convention as
                # taskq.worker.deps). Read off app.state, not the local: a
                # rotation may have replaced the pool built above.
                await close_pool_bounded(application.state.pg_pool, "ui-admin", CLOSE_TIMEOUT_SECS)

            # Why a pushed callback instead of stack.enter_async_context(pool):
            # Pool.__aexit__ closes UNBOUNDED, a dead PG would wedge UI
            # shutdown. The bounded helper terminates the pool on
            # timeout and never raises.
            stack.push_async_callback(_close_ui_pool)

            redis_client: object | None = None
            if redis_url is not None:
                if redis_factory is not None:
                    # Why bounded: same first-use factory discipline as the
                    # pool factory above, the worker's reload bounds its
                    # identical redis factory call with reload_factory_timeout
                    # (worker/deps.py); a hung Redis credential provider must
                    # fail UI startup loudly, not park it forever.
                    try:
                        client = await asyncio.wait_for(
                            redis_factory(), timeout=_UI_FACTORY_TIMEOUT_SECS
                        )
                    except TimeoutError as exc:
                        raise TimeoutError(
                            f"taskq ui serve: redis_factory did not return "
                            f"within {_UI_FACTORY_TIMEOUT_SECS}s, the Redis "
                            "credential provider is black-holed. UI startup "
                            "fails loudly instead of parking forever."
                        ) from exc
                else:
                    from taskq._redis_client import build_redis_client

                    try:
                        client = build_redis_client(redis_url)
                    except ImportError as exc:
                        raise ImportError(
                            "redis_url is configured but the [redis] extra is not installed. "
                            "Install it with: pip install 'taskq-py[redis]'"
                        ) from exc

                # Why not stack.enter_async_context(client): Redis.__aexit__
                # calls aclose() UNBOUNDED (and shielded), a hung broker
                # would wedge UI shutdown. initialize()
                # preserves __aenter__'s eager-setup semantics; the pushed
                # callback bounds the close instead (taskq._close
                # pattern; redis has no terminate(), so it is
                # log-and-continue).
                async def _close_ui_redis() -> None:
                    # Why module-global reads at call time: tests monkeypatch
                    # close_redis_bounded / CLOSE_TIMEOUT_SECS as
                    # observation and timeout-shrink seams (same convention as
                    # the pool close above).
                    await close_redis_bounded(client, "ui-admin", CLOSE_TIMEOUT_SECS)

                # Why push BEFORE initialize(): from_url() has already
                # allocated the connection pool, so if initialize() raises
                # (broker down) the failed eager setup must still release it
                # , the unwind runs the pushed callback through the bounded
                # close (never raises; aclose() on a never-initialized client
                # is a no-op).
                stack.push_async_callback(_close_ui_redis)
                # Why bounded: initialize() is the eager first broker round
                # trip, a black-holed Redis would park UI startup forever,
                # and the UI process arms no watchdog. Same wait_for
                # discipline as JobsClient._open_redis; the pushed callback
                # above already bounds the unwind's close.
                try:
                    await asyncio.wait_for(client.initialize(), timeout=_UI_FACTORY_TIMEOUT_SECS)
                except TimeoutError as exc:
                    raise TimeoutError(
                        f"taskq ui serve: Redis initialize() did not complete "
                        f"within {_UI_FACTORY_TIMEOUT_SECS}s, the broker at "
                        f"{redis_url} is unreachable or black-holed. UI "
                        "startup fails loudly instead of parking forever."
                    ) from exc
                redis_client = client

            bundle = create_router(
                application.state.pg_pool,
                schema=schema,
                redis_client=redis_client,
                auth_dependency=auth_dependency,
                base_path="/admin",
            )

            setup_admin_state(application, bundle)
            # Armed only now: setup_admin_state copies the bundle's pool onto
            # app.state, so a rotation running before this point could be
            # undone by that copy.
            if pool_factory is not None:
                await stack.enter_async_context(
                    _ui_credential_rotation(application, pool_factory, settings)
                )
            application.include_router(bundle.router, prefix="/admin")
            if sso_bundle is not None:
                application.include_router(sso_bundle.router, prefix="/admin")

            health_router = APIRouter(
                prefix="/jobs/health",
                tags=["health"],
                dependencies=health_deps,
                # F6: the monitors' HEAD checks against the health
                # endpoints must not 405.
                route_class=HeadForGetRoute,
            )

            @health_router.get("/live")
            async def _health_live() -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
                ok, _msg = await _check_live()
                body_dict: dict[str, str] = {"status": "ok"} if ok else {"status": "unresponsive"}
                return Response(
                    content=_json.dumps(body_dict),
                    media_type="application/json",
                    status_code=200 if ok else 503,
                )

            @health_router.get("/ready")
            async def _health_ready() -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
                t0 = time.perf_counter()
                try:
                    # Why bounded: the worker's readiness ping bounds the
                    # identical probe (acquire + SELECT 1) with
                    # health_pg_ping_timeout (worker/health.py); an
                    # unbounded probe turns a wedged pool or a black-holed
                    # PG into a wedged prober. Resolved per probe: the pool
                    # is replaced by a credential rotation.
                    pool = _ui_live_pool(application)
                    async with pool.acquire(timeout=_UI_PG_PING_TIMEOUT_SECS) as conn:
                        await asyncio.wait_for(
                            conn.execute("SELECT 1"),
                            timeout=_UI_PG_PING_TIMEOUT_SECS,
                        )
                    ok = True
                    reasons: list[str] = []
                except TimeoutError:
                    ok = False
                    reasons = ["pg_ping_timeout"]
                except Exception:
                    ok = False
                    reasons = ["pg_connection_error"]
                latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
                ready_body: dict[str, object] = {
                    "ready": ok,
                    "reasons": reasons,
                    "pg_ping_ok": ok,
                    "pg_ping_latency_ms": latency_ms,
                    "redis_configured": redis_url is not None,
                }
                return Response(
                    content=_json.dumps(ready_body),
                    media_type="application/json",
                    status_code=200 if ok else 503,
                )

            application.include_router(health_router)

            try:
                from taskq.contrib.prometheus import (
                    create_metrics_router,  # pyright: ignore[reportUnknownVariableType]  # Why: prometheus_client ships no type stubs; the function signature includes registry: CollectorRegistry with unknown type.
                )

                metrics_router = create_metrics_router(None)  # pyright: ignore[reportArgumentType, reportUnknownVariableType]  # Why: _deps is unused by create_metrics_router (signature parity with create_health_router only); the standalone UI server has no WorkerDeps to pass.
                application.include_router(
                    metrics_router,
                    prefix="/jobs/health",
                    dependencies=health_deps,
                )
            except ImportError:
                pass

            yield

    app = FastAPI(lifespan=lifespan)
    from taskq.web.admin._factory import GZipStaticOnly

    app.add_middleware(GZipStaticOnly, minimum_size=500)

    @app.get("/", include_in_schema=False)
    def _root_redirect() -> RedirectResponse:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        return RedirectResponse(url="/admin/", status_code=307)

    import uvicorn

    uvicorn.run(app, host=host, port=port)


@ui_app.command("serve")
def ui_serve(
    pg_dsn: str | None = typer.Option(
        None,
        "--pg-dsn",
        help="Postgres DSN. Falls back to TASKQ_PG_DSN via dotenvmodel.",
    ),
    schema: str | None = typer.Option(
        None,
        "--schema",
        help="Postgres schema name. Falls back to TASKQ_SCHEMA_NAME via dotenvmodel.",
    ),
    redis_url: str | None = typer.Option(
        None,
        "--redis-url",
        help="Redis URL for real-time mode. Falls back to TASKQ_REDIS_URL via dotenvmodel.",
    ),
    host: str | None = typer.Option(
        None,  # pyright: ignore[reportArgumentType]  # Why: None signals "use settings default"; resolved below before passing to uvicorn.
        "--host",
        help="Bind address. Falls back to TASKQ_ADMIN_HOST via dotenvmodel.",
    ),
    port: int | None = typer.Option(
        None,  # pyright: ignore[reportArgumentType]  # Why: None signals "use settings default"; resolved below before passing to uvicorn.
        "--port",
        help="Bind port. Falls back to TASKQ_ADMIN_PORT via dotenvmodel.",
    ),
    run_migrate: bool = typer.Option(
        False,
        "--migrate",
        help="Apply pending migrations before starting. Aborts startup if migrations fail.",
    ),
    pg_credential_provider: str | None = typer.Option(
        None,
        "--pg-credential-provider",
        help="Module:attr reference to a PgCredentialProvider (e.g. "
        f"{_PROVIDER_EXAMPLE}). The admin pool (and --migrate) authenticate "
        "through it instead of the DSN's static password. Overrides "
        "TASKQ_PG_CREDENTIAL_PROVIDER.",
    ),
    redis_credential_provider: str | None = typer.Option(
        None,
        "--redis-credential-provider",
        help="Module:attr reference to a RedisCredentialProvider for the real-time "
        "mode client. Overrides TASKQ_REDIS_CREDENTIAL_PROVIDER.",
    ),
) -> None:
    """Start the admin UI server on the given host:port."""
    settings = TaskQSettings.load()

    resolved_dsn = pg_dsn if pg_dsn is not None else str(settings.pg_dsn)
    resolved_schema = schema if schema is not None else settings.schema_name
    resolved_redis = (
        redis_url
        if redis_url is not None
        else (str(settings.redis_url) if settings.redis_url is not None else None)
    )
    resolved_host = host if host is not None else settings.admin_host
    resolved_port = port if port is not None else settings.admin_port
    resolved_migrate = run_migrate or settings.migrate_on_start

    resolved_pg_provider_ref = _resolved_ref(
        pg_credential_provider, settings.pg_credential_provider
    )
    resolved_redis_provider_ref = _resolved_ref(
        redis_credential_provider, settings.redis_credential_provider
    )

    pool_factory: PoolFactory | None = None
    conn_factory: ConnFactory | None = None
    providers: list[object] = []
    if resolved_pg_provider_ref is not None:
        pg_provider = _load_pg_credential_provider(
            resolved_pg_provider_ref, option="--pg-credential-provider"
        )
        providers.append(pg_provider)
        # Why command_timeout: this factory builds the UI's admin pool ,
        # the factory-path twin of the create_pool bound in the lifespan,
        # or the credential-provider deployment would be the one unbounded
        # admin pool left standing. (The migrate conn_factory below is
        # deliberately NOT bounded: DDL may legitimately exceed a
        # per-query budget.)
        pool_factory = make_pg_pool_factory(
            resolved_dsn,
            pg_provider,
            max_size=4,
            command_timeout=_UI_POOL_COMMAND_TIMEOUT_SECS,
            # The cadence the lifespan's rotation loop rebuilds this pool
            # on: TASKQ_RELOAD_INTERVAL when set, else derived from the
            # lease the provider grants (see ReloadSchedule).
            reload_schedule=ReloadSchedule(configured=settings.reload_interval),
        )
        # One-shot: the migration connection is opened, used and closed at
        # startup, so it declares no long-lived schedule.
        conn_factory = make_dedicated_conn_factory(resolved_dsn, pg_provider)

    redis_factory: RedisFactory | None = None
    if resolved_redis_provider_ref is not None:
        if resolved_redis is None:
            typer.echo(
                "--redis-credential-provider was given but no Redis URL is set - "
                "set TASKQ_REDIS_URL (or --redis-url), or drop the Redis provider.",
                err=True,
            )
            raise typer.Exit(code=1)
        redis_provider = _load_redis_credential_provider(
            resolved_redis_provider_ref, option="--redis-credential-provider"
        )
        providers.append(redis_provider)
        redis_factory = make_redis_client_factory(resolved_redis, redis_provider)

    _ui_serve(
        resolved_dsn,
        resolved_schema,
        resolved_redis,
        resolved_host,
        resolved_port,
        resolved_migrate,
        settings,
        pool_factory=pool_factory,
        conn_factory=conn_factory,
        redis_factory=redis_factory,
        # One instance may serve both roles (EntraIdProvider); closed once.
        providers=list(dict.fromkeys(providers)),
    )


def _ensure_cwd_on_sys_path() -> None:
    """Put the current working directory on ``sys.path`` if it is absent.

    A console script starts with a ``sys.path`` that excludes the cwd, so
    ``taskq worker --actors myapp.actors:registry`` could not resolve the
    application's modules when run from its own project directory.
    ``python -m taskq`` prepends the cwd itself; inserting it here gives
    the console script the same import semantics any ``python -m``
    invocation gets before any ``module:attr`` resolution runs.
    """
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def main() -> None:
    """Console-script entry point."""
    _ensure_cwd_on_sys_path()
    app()


@workgroup_app.command("start")
def workgroup_start(
    config: Annotated[
        Path,
        typer.Argument(help="Path to the workgroup TOML configuration file."),
    ],
) -> None:
    """Start a workgroup supervisor that manages multiple worker processes.

    The supervisor spawns one ``taskq worker`` subprocess per ``[[workers]]``
    entry in the config file, monitors their health, restarts them on crash,
    and propagates shutdown signals.
    """
    if not config.exists():
        typer.echo(f"config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    from taskq.worker.workgroup import run_forever

    asyncio.run(run_forever(config))


@workgroup_app.command("validate")
def workgroup_validate(
    config: Annotated[
        Path,
        typer.Argument(help="Path to the workgroup TOML configuration file."),
    ],
) -> None:
    """Validate a workgroup TOML config without starting any workers."""
    if not config.exists():
        typer.echo(f"config file not found: {config}", err=True)
        raise typer.Exit(code=1)

    import tomllib

    from taskq.worker.workgroup import load_workgroup_config

    try:
        cfg = load_workgroup_config(config)
    except (ValueError, tomllib.TOMLDecodeError) as e:
        typer.echo(f"invalid config: {e}", err=True)
        raise typer.Exit(code=1) from None
    except OSError as e:
        typer.echo(f"failed to read config: {e}", err=True)
        raise typer.Exit(code=1) from None

    typer.echo(f"config OK, {len(cfg.workers)} worker(s), actors={cfg.actors!r}")
    for w in cfg.workers:
        health = "health=on" if w.health.enabled else "health=off"
        typer.echo(
            f"  {w.name}: queues={w.queues} "
            f"poll={w.poll_interval}s concurrency={w.max_concurrency} {health}"
        )


# ── queues ─────────────────────────────────────────────────────────────


def _print_queue_row(row: QueueRow) -> None:
    cap = "unlimited" if row.max_concurrent is None else str(row.max_concurrent)
    typer.echo(f"{row.name}  mode={row.mode}  max_concurrent={cap}")


@queues_app.command("list")
def queues_list() -> None:
    """List every configured queue row.

    Queues absent from this list are not missing -- queues are implicit and
    are created by enqueueing onto them. An absent queue runs on the
    defaults: strict_fifo ordering (so `fairness_key` has no effect) and no
    concurrency cap.
    """
    settings = TaskQSettings.load()
    asyncio.run(_queues_list(settings))


async def _queues_list(settings: TaskQSettings) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await list_queues(conn, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "queues-list", CLOSE_TIMEOUT_SECS)
    if not rows:
        typer.echo(
            "no configured queues (all queues run on defaults: "
            "mode=strict_fifo, max_concurrent=unlimited)"
        )
        return
    for row in rows:
        _print_queue_row(row)


@queues_app.command("get")
def queues_get(
    name: Annotated[str, typer.Argument(help="Queue name.")],
) -> None:
    """Show one queue's stored configuration."""
    settings = TaskQSettings.load()
    asyncio.run(_queues_get(settings, name))


async def _queues_get(settings: TaskQSettings, name: str) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        row = await get_queue(conn, name, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "queues-get", CLOSE_TIMEOUT_SECS)
    if row is None:
        typer.echo(
            f"queue {name!r} has no stored row; it runs on defaults "
            "(mode=strict_fifo, max_concurrent=unlimited). "
            "fairness_key has NO effect on a strict_fifo queue."
        )
        return
    _print_queue_row(row)


@queues_app.command("set-mode")
def queues_set_mode(
    name: Annotated[str, typer.Argument(help="Queue name.")],
    mode: Annotated[
        str,
        typer.Argument(help=f"Dispatch ordering mode. One of: {', '.join(QUEUE_MODES)}."),
    ],
) -> None:
    """Set a queue's dispatch ordering mode, creating the row if needed.

    `round_robin` is what makes `fairness_key` do anything: on the default
    `strict_fifo` the key is accepted, stored, and ignored. Takes effect on
    the next dispatch cycle -- no worker restart.
    """
    settings = TaskQSettings.load()
    asyncio.run(_queues_set_mode(settings, name, mode))


async def _queues_set_mode(settings: TaskQSettings, name: str, mode: str) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        try:
            row = await set_queue_mode(conn, name, mode, schema=settings.schema_name)
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
    finally:
        await close_conn_bounded(conn, "queues-set-mode", CLOSE_TIMEOUT_SECS)
    _print_queue_row(row)


@queues_app.command("set-max-concurrent")
def queues_set_max_concurrent(
    name: Annotated[str, typer.Argument(help="Queue name.")],
    max_concurrent: Annotated[
        int | None,
        typer.Option(
            "--max-concurrent",
            min=1,
            help="New per-queue leased-slot cap (>= 1; pass --clear for uncapped).",
        ),
    ] = None,
    clear: Annotated[bool, typer.Option("--clear", help="Remove the cap (unlimited).")] = False,
) -> None:
    """Set or clear a queue's fleet-wide leased-slot concurrency cap.

    Unlike `actor-config set --max-concurrent`, this is read once at worker
    startup, so it needs a worker restart to take effect. There is no 0
    state: NULL (via --clear) is uncapped, and an emergency drain to 0
    belongs to `actor-config set --max-concurrent 0`, which is per-actor.
    """
    if clear and max_concurrent is not None:
        typer.echo("pass either --max-concurrent or --clear, not both", err=True)
        raise typer.Exit(code=1)
    if not clear and max_concurrent is None:
        typer.echo("pass --max-concurrent N or --clear", err=True)
        raise typer.Exit(code=1)
    settings = TaskQSettings.load()
    asyncio.run(_queues_set_max_concurrent(settings, name, None if clear else max_concurrent))


async def _queues_set_max_concurrent(
    settings: TaskQSettings, name: str, max_concurrent: int | None
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        try:
            row = await set_queue_max_concurrent(
                conn, name, max_concurrent, schema=settings.schema_name
            )
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
    finally:
        await close_conn_bounded(conn, "queues-set-max-concurrent", CLOSE_TIMEOUT_SECS)
    _print_queue_row(row)


_DEPTH_SQL_TEMPLATE = (
    "SELECT queue, "
    "count(*) FILTER (WHERE status = 'pending')::int AS pending, "
    "count(*) FILTER (WHERE status = 'scheduled')::int AS scheduled, "
    "count(*) FILTER (WHERE status = 'running')::int AS running, "
    "count(*) FILTER (WHERE status = 'failed')::int AS failed, "
    "EXTRACT(EPOCH FROM (clock_timestamp() "
    "- MIN(created_at) FILTER (WHERE status = 'pending')))::float8 AS oldest_pending_age "
    'FROM "{schema}".jobs '
    "GROUP BY queue "
    "ORDER BY max(created_at) DESC, queue ASC"
)
"""One grouped pass over jobs, per queue: the four depth counts plus the
oldest pending row's age.

The shape reuses the leader sampler's reads: grouped counts over the
live statuses with the age arithmetic done server-side
(``clock_timestamp()``), never by subtracting this process's clock from
a database timestamp. ``failed`` is the one deliberate widening beyond
the sampler's live-status set: the sampler drops terminal statuses
because a per-tick read must not grow with the terminal history between
retention sweeps, while this command runs once, on demand, and an
operator reading depth wants the failure signal next to the backlog.
Ordering is newest activity first (the queue with the most recent job
on top), queue name breaking ties."""


@queues_app.command("depth")
def queues_depth() -> None:
    """Per-queue depth: pending/scheduled/running/failed counts and the
    age of the oldest pending job, newest activity first."""
    settings = TaskQSettings.load()
    asyncio.run(_queues_depth(settings))


async def _queues_depth(settings: TaskQSettings) -> None:
    if not _IDENT_RE.match(settings.schema_name):
        # Defence in depth: the queue_ops convention, re-checked at the
        # SQL interpolation site.
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)
    sql = _DEPTH_SQL_TEMPLATE.format(schema=settings.schema_name)
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await conn.fetch(
            sql
        )  # Why: sql is a rendered constant, the schema the only interpolation, identifier-validated above.
    finally:
        await close_conn_bounded(conn, "queues-depth", CLOSE_TIMEOUT_SECS)
    if not rows:
        typer.echo(f"no jobs in {settings.schema_name}.jobs; nothing to report")
        return
    name_width = max(len("queue"), max(len(str(row["queue"])) for row in rows))
    header = f"{'queue':{name_width}}  pending  scheduled  running  failed  oldest_pending"
    typer.echo(header)
    for row in rows:
        age = "-" if row["oldest_pending_age"] is None else _format_age(row["oldest_pending_age"])
        typer.echo(
            f"{row['queue']:{name_width}}  {row['pending']:>7}  {row['scheduled']:>9}  "
            f"{row['running']:>7}  {row['failed']:>6}  {age:>14}"
        )


def _format_age(seconds: float) -> str:
    """Humanize a server-computed age for the depth table.

    The CLI's presentation contract over the shared
    :func:`taskq._humantime.humanize_age` split — one cascade, this
    surface's compact no-space shape on top.
    """
    return humanize_age(seconds)


def _format_insights_duration(seconds: float | None) -> str:
    """Humanize an insights duration (a wait percentile, an eta, an age).

    Sub-10-second values keep one decimal — a p50 of 0.4s is the healthy
    shape and rounding it to ``0s`` would erase the signal; ``-`` renders
    the NULL case (no observation), never a zero that would read as
    "instant".
    """
    if seconds is None:
        return "-"
    if seconds < 10:
        return f"{seconds:.1f}s"
    return _format_age(seconds)


# ── insights ───────────────────────────────────────────────────────────
#
# The terminal surface for taskq.insights (the pure-read SQL layer over
# the job ledger). NO hand-written SQL here: every row the command prints
# comes from the module's fetchers, so the CLI and any other consumer of
# the layer answer the operator's question with one definition. The
# doctor's read-only discipline holds: the fetchers are SELECT-only, the
# command exits 0 whether or not there is anything to report, and the
# confounds the module documents in its docstrings are carried into the
# rendered tables and into this command's --help.

_INSIGHTS_SURFACES: Final = ("wait", "balance", "drain", "cron", "all")
"""The surface argument's closed set; ``all`` (the default) renders every one."""

_INSIGHTS_NO_CAPACITY_MARKER: Final[str] = "!! no capacity"
_INSIGHTS_OVER_THRESHOLD_MARKER: Final[str] = "!! over threshold"
_INSIGHTS_RUNAWAY_MARKER: Final[str] = "!! runaway trending"
_INSIGHTS_NO_TRAFFIC_ETA: Final[str] = "no traffic in window"


@app.command("insights")
def insights(
    surface: Annotated[
        str,
        typer.Argument(
            help=f"Which insight to render: one of {', '.join(_INSIGHTS_SURFACES)}. Default: all."
        ),
    ] = "all",
    window: Annotated[
        str,
        typer.Option(
            "--window",
            help=f"Trailing window for the windowed reads (wait, drain, cron). One of: "
            f"{', '.join(INSIGHTS_WINDOWS)}. A window older than the archive retention "
            "answers over the surviving chunks — the retention floor is the analytics floor.",
        ),
    ] = "1h",
    actor: Annotated[
        str | None,
        typer.Option(
            "--actor",
            help="wait only: switch the wait distribution to the per-(actor, queue) grouping "
            "and keep this actor's rows. The plain per-queue read folds every actor's "
            "attempts together; the actor view separates them.",
        ),
    ] = None,
    queue: Annotated[
        str | None,
        typer.Option(
            "--queue",
            help="Keep only this queue's rows (wait, balance, drain). Refused on cron: the "
            "fan-out ledger is per schedule and has no queue dimension.",
        ),
    ] = None,
) -> None:
    """Render the operational-insights SQL layer as operator tables.

    Read-only like `taskq doctor`: every statement is a SELECT over BOTH
    retention tiers (live `jobs` + `jobs_archive`), the command issues no
    write, and it exits 0 whether or not there is anything to report.
    Safe to run against production mid-incident.

    The surfaces and their columns:

    * `wait` — how long terminalised attempts WAITED before their claim,
      per queue. Columns: `count` (terminalised attempts in the window),
      `p50`/`p95`/`max` (the wait distribution), `segment`. Wait per
      attempt is `started_at - scheduled_at` (the dispatch claim
      latency). `segment` = `clean` for first-delivery attempts
      (`snooze_count = 0` and `rate_limit_blocked_count = 0`) — the
      subset a queue-latency SLO is written against; `segment` =
      `deferred` for rows whose `scheduled_at` a snooze or a rate limit
      moved forward: RESCHEDULES ARE EXCLUDED from the clean
      percentiles, and a deferred row's wait measures only the FINAL
      leg (the deferred time is by construction not in the number).
      `--actor` switches to the per-(actor, queue) grouping and keeps
      one actor's rows.

    * `balance` — is the fleet imbalanced, per queue. Columns: `depth`
      (pending rows due now — the work a worker could claim this
      instant), `live` (workers subscribed to the queue and heartbeating
      in the last 30s), `effective_capacity` (the routed actors'
      summed `max_concurrent` x live workers: the queue's in-flight
      ceiling under the current fleet), `utilization` (depth ÷
      effective capacity), `oldest_due` (age of the oldest due job:
      a large depth with a tiny age is a burst, a small depth with a
      large age is a strand), `flag`. Rows with utilization > 1 are
      MARKED `!! over threshold` (more due work than one wave of
      capacity can absorb); `!! no capacity` marks the starvation shape
      (no live worker or no actor capacity serves the queue).

    * `drain` — seconds-to-drain, per queue: due depth ÷ completions
      per second over the window. This is a THROUGHPUT extrapolation,
      not a promise: it assumes the next window looks like the last
      one, that the queue's workers stay up, and that nothing enqueues
      behind the current depth. A queue whose window carried no
      completions has NO estimate: its eta renders `no traffic in
      window` — never 0, which would read as "already drained". Widen
      `--window` (up to the archive retention floor) before trusting
      anything else.

    * `cron` — the per-schedule fan-out ledger. Columns: `schedule`
      (the schedule id, shortened), `actor`, `cron`, `fires` (jobs the
      schedule enqueued in the window), `cleared` (of those,
      terminalised), `outstanding` (the schedule's non-terminal backlog
      right now, windowless), `verdict`. `!! runaway trending` marks
      fires > cleared in BOTH the current and the prior equal window —
      two consecutive windows of fan-out outrunning clearance is the
      runaway shape; one window is a burst. Confounds: a DST `allof`
      schedule legitimately doubles a fire in the overlap hour (read
      the verdict against the schedule's dst_strategy), and a
      budget-deferred fire enqueues NO row, so a deferral reads as zero
      fires here — its record is the `taskq.cron.budget_deferrals`
      counter, not this ledger.

    * `all` (the default) — every surface, in the order above.
    """
    if surface not in _INSIGHTS_SURFACES:
        typer.echo(
            f"unknown insights surface: {surface!r} "
            f"(expected one of: {', '.join(_INSIGHTS_SURFACES)})",
            err=True,
        )
        raise typer.Exit(code=1)
    window_delta = INSIGHTS_WINDOWS.get(window)
    if window_delta is None:
        typer.echo(
            f"invalid --window: {window!r} (expected one of: {', '.join(INSIGHTS_WINDOWS)})",
            err=True,
        )
        raise typer.Exit(code=1)
    if actor is not None and surface not in ("wait", "all"):
        typer.echo(
            "--actor applies to the wait surface only: the wait distribution is the "
            "one read with an actor dimension",
            err=True,
        )
        raise typer.Exit(code=1)
    if queue is not None and surface == "cron":
        typer.echo(
            "--queue does not apply to the cron surface: the fan-out ledger is per "
            "schedule and has no queue dimension",
            err=True,
        )
        raise typer.Exit(code=1)
    settings = TaskQSettings.load()
    asyncio.run(_insights(settings, surface, window, window_delta, actor, queue))


async def _insights(
    settings: TaskQSettings,
    surface: str,
    window_name: str,
    window_delta: timedelta,
    actor: str | None,
    queue: str | None,
) -> None:
    """Run the requested surfaces on one connection and render each.

    The schema identifier is re-checked at this boundary (the queue_ops
    convention: TaskQSettings validates at load, the SQL site re-checks);
    below this check the insights module's own fetchers own every
    statement, so no hand-written SQL exists on this path.
    """
    if not _IDENT_RE.match(settings.schema_name):
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        first = True
        if surface in ("wait", "all"):
            await _insights_wait(
                conn,
                schema=settings.schema_name,
                window=window_delta,
                window_name=window_name,
                actor=actor,
                queue=queue,
            )
            first = False
        if surface in ("balance", "all"):
            if not first:
                typer.echo("")
            await _insights_balance(conn, schema=settings.schema_name, queue=queue)
            first = False
        if surface in ("drain", "all"):
            if not first:
                typer.echo("")
            await _insights_drain(
                conn,
                schema=settings.schema_name,
                window=window_delta,
                window_name=window_name,
                queue=queue,
            )
            first = False
        if surface in ("cron", "all"):
            if not first:
                typer.echo("")
            await _insights_cron(
                conn, schema=settings.schema_name, window=window_delta, window_name=window_name
            )
    finally:
        await close_conn_bounded(conn, "insights", CLOSE_TIMEOUT_SECS)


def _insights_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    """Render one insights table: first column left-aligned, the rest
    right-aligned, two-space gutters (the `queues depth` table's idiom,
    generalized over the four surfaces' column sets)."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def line(cells: Sequence[str]) -> str:
        first = cells[0].ljust(widths[0])
        rest = "  ".join(cell.rjust(widths[i]) for i, cell in enumerate(cells[1:], start=1))
        return f"{first}  {rest}" if rest else first

    typer.echo(line(headers))
    for row in rows:
        typer.echo(line(row))


async def _insights_wait(
    conn: Any,
    *,
    schema: str,
    window: timedelta,
    window_name: str,
    actor: str | None,
    queue: str | None,
) -> None:
    """The `wait` surface: the module's wait distribution, one row per
    (queue, segment) — or per (actor, queue, segment) under --actor."""
    rows = await fetch_wait_distribution(
        conn, schema=schema, window=window, per_actor=actor is not None
    )
    if actor is not None:
        rows = [r for r in rows if r["actor"] == actor]
    if queue is not None:
        rows = [r for r in rows if r["queue"] == queue]
    typer.echo(
        f"wait (window {window_name}): how long terminalised attempts waited before "
        "their claim (started_at - scheduled_at)"
    )
    if not rows:
        typer.echo("  no terminal job activity in the window; nothing to report")
        return
    headers = (
        ["queue", "actor", "segment", "count", "p50", "p95", "max"]
        if actor is not None
        else ["queue", "segment", "count", "p50", "p95", "max"]
    )
    table: list[list[str]] = []
    for r in rows:
        cells = [str(r["queue"])]
        if actor is not None:
            cells.append(str(r["actor"]))
        cells += [
            str(r["segment"]),
            str(r["count"]),
            _format_insights_duration(r["p50_wait_s"]),
            _format_insights_duration(r["p95_wait_s"]),
            _format_insights_duration(r["max_wait_s"]),
        ]
        table.append(cells)
    _insights_table(headers, table)
    typer.echo(
        "  segment clean = first-delivery attempts (snooze_count = 0 and "
        "rate_limit_blocked_count = 0); deferred rows' scheduled_at was moved"
    )
    typer.echo(
        "  forward by a snooze or rate limit, so their wait covers only the "
        "final leg; reschedules are excluded from the clean percentiles."
    )


async def _insights_balance(conn: Any, *, schema: str, queue: str | None) -> None:
    """The `balance` surface: the module's per-queue fleet imbalance, with
    the over-threshold and starvation shapes marked."""
    rows = await fetch_queue_imbalance(conn, schema=schema)
    if queue is not None:
        rows = [r for r in rows if r["queue"] == queue]
    typer.echo("balance: per-queue fleet imbalance (live = workers heartbeating in the last 30s)")
    if not rows:
        typer.echo("  no queues with jobs, workers, or actor capacity; nothing to report")
        return
    table: list[list[str]] = []
    for r in rows:
        utilization = r["utilization"]
        if utilization is None:
            util_text = "-"
            flag = _INSIGHTS_NO_CAPACITY_MARKER
        else:
            util_text = f"{utilization:.2f}"
            flag = _INSIGHTS_OVER_THRESHOLD_MARKER if utilization > 1 else ""
        age = (
            "-"
            if r["oldest_due_age_s"] is None
            else _format_insights_duration(r["oldest_due_age_s"])
        )
        table.append(
            [
                str(r["queue"]),
                str(r["depth"]),
                str(r["live_workers"]),
                str(r["effective_capacity"]),
                util_text,
                age,
                flag,
            ]
        )
    _insights_table(
        ["queue", "depth", "live", "effective_capacity", "utilization", "oldest_due", "flag"],
        table,
    )


async def _insights_drain(
    conn: Any,
    *,
    schema: str,
    window: timedelta,
    window_name: str,
    queue: str | None,
) -> None:
    """The `drain` surface: the module's seconds-to-drain estimate, with
    the no-traffic caveat rendered honestly (never an eta of 0)."""
    rows = await fetch_drain_estimates(conn, schema=schema, window=window)
    if queue is not None:
        rows = [r for r in rows if r["queue"] == queue]
    typer.echo(
        f"drain (window {window_name}): seconds-to-drain, due depth over the window's "
        "realized throughput (a throughput extrapolation, not a promise)"
    )
    if not rows:
        typer.echo("  no queues with jobs; nothing to report")
        return
    table: list[list[str]] = []
    for r in rows:
        eta = (
            _format_insights_duration(r["eta_seconds"])
            if r["has_traffic"]
            else _INSIGHTS_NO_TRAFFIC_ETA
        )
        table.append([str(r["queue"]), str(r["depth"]), eta])
    _insights_table(["queue", "depth", "eta"], table)


async def _insights_cron(
    conn: Any,
    *,
    schema: str,
    window: timedelta,
    window_name: str,
) -> None:
    """The `cron` surface: the module's per-schedule fan-out ledger, with
    the two-consecutive-windows runaway verdict marked."""
    rows = await fetch_cron_ledger(conn, schema=schema, window=window)
    typer.echo(
        f"cron (window {window_name}): per-schedule fan-out ledger (cleared lags "
        "fires at the window's right edge; the verdict is a trend, not an instant)"
    )
    if not rows:
        typer.echo("  no cron schedules; nothing to report")
        return
    table: list[list[str]] = []
    for r in rows:
        verdict = _INSIGHTS_RUNAWAY_MARKER if r["runaway_trending"] else "ok"
        table.append(
            [
                str(r["schedule_id"])[:8],
                str(r["actor"]),
                str(r["cron_expr"]),
                str(r["fires_window"]),
                str(r["cleared_window"]),
                str(r["outstanding"]),
                verdict,
            ]
        )
    _insights_table(
        ["schedule", "actor", "cron", "fires", "cleared", "outstanding", "verdict"], table
    )


# ── job ────────────────────────────────────────────────────────────────

_JOB_SHOW_COLUMNS: Final = (
    "id",
    "actor",
    "queue",
    "status",
    "priority",
    "attempt",
    "max_attempts",
    "retry_kind",
    "created_at",
    "scheduled_at",
    "started_at",
    "finished_at",
    "error_class",
    "error_message",
    "idempotency_key",
)
"""The operator-facing columns ``taskq job show`` prints by default.

Explicit, not ``SELECT *``: the printed set is the contract, and leaving
``payload``/``result``/``progress_state``/``error_traceback`` out keeps a
terminal-friendly read from dragging arbitrarily large blobs onto the
wire. Both ``jobs`` and ``jobs_archive`` carry every column listed.
``error_traceback`` and ``payload`` join the SELECT only under the
opt-in ``--traceback``/``--payload`` flags (see ``job_show``): the
default output stays blob-free, the flags are the operator asking for
the blob by name.
"""


def _format_max_attempts(max_attempts: int, retry_kind: str) -> str:
    """The ``max_attempts`` display for a job row.

    Under ``retry_kind='indefinite'`` the stored ceiling is inert, the
    retry path never consults it, so printing the number would claim a
    budget the job does not carry. Render the inertness instead, the same
    framing the retries guide gives the field on an indefinite job.
    """
    if parse_retry_kind(retry_kind) == "indefinite":
        return "— (indefinite)"
    return str(max_attempts)


@job_app.command("show")
def job_show(
    job_id: Annotated[str, typer.Argument(help="Job id (UUID).")],
    show_traceback: Annotated[
        bool,
        typer.Option("--traceback", help="Also print the stored error_traceback (can be large)."),
    ] = False,
    show_payload: Annotated[
        bool,
        typer.Option("--payload", help="Also print the stored payload jsonb (can be large)."),
    ] = False,
) -> None:
    """Show one job's stored row, from `jobs` or `jobs_archive`."""
    settings = TaskQSettings.load()
    asyncio.run(
        _job_show(settings, job_id, show_traceback=show_traceback, show_payload=show_payload)
    )


async def _job_show(
    settings: TaskQSettings,
    job_id: str,
    *,
    show_traceback: bool = False,
    show_payload: bool = False,
) -> None:
    try:
        parsed = UUID(job_id)
    except ValueError:
        typer.echo(f"invalid job id (expected a UUID): {job_id!r}", err=True)
        raise typer.Exit(code=1) from None
    if not _IDENT_RE.match(settings.schema_name):
        # Defence in depth: TaskQSettings validates schema_name at load;
        # re-check at the SQL interpolation site (the queue_ops convention).
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)
    columns = ", ".join(_JOB_SHOW_COLUMNS)
    # Opt-in columns only: the default SELECT stays blob-free (see
    # _JOB_SHOW_COLUMNS), the flags add the named blob to the same probe.
    extra_columns = ""
    if show_traceback:
        extra_columns += ", error_traceback"
    if show_payload:
        extra_columns += ", payload"
    columns += extra_columns
    conn = await asyncpg.connect(str(settings.pg_dsn))
    archived = False
    try:
        row = await conn.fetchrow(
            f'SELECT {columns} FROM "{settings.schema_name}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is identifier-validated above; asyncpg cannot bind identifiers.
            parsed,
        )
        if row is None:
            row = await conn.fetchrow(
                f'SELECT {columns} FROM "{settings.schema_name}".jobs_archive WHERE id = $1',  # noqa: S608  # Why: schema is identifier-validated above; asyncpg cannot bind identifiers.
                parsed,
            )
            archived = row is not None
    finally:
        await close_conn_bounded(conn, "job-show", CLOSE_TIMEOUT_SECS)
    if row is None:
        typer.echo(
            f"no job {parsed} in {settings.schema_name}.jobs or "
            f"{settings.schema_name}.jobs_archive",
            err=True,
        )
        raise typer.Exit(code=1)

    typer.echo(f"id: {row['id']}")
    typer.echo(f"actor: {row['actor']}")
    typer.echo(f"queue: {row['queue']}")
    typer.echo(f"status: {row['status']}")
    typer.echo(f"priority: {row['priority']}")
    typer.echo(f"attempt: {row['attempt']}")
    typer.echo(f"max_attempts: {_format_max_attempts(row['max_attempts'], row['retry_kind'])}")
    typer.echo(f"retry_kind: {row['retry_kind']}")
    typer.echo(f"created_at: {row['created_at']}")
    typer.echo(f"scheduled_at: {row['scheduled_at']}")
    typer.echo(f"started_at: {row['started_at']}")
    typer.echo(f"finished_at: {row['finished_at']}")
    if row["error_class"] is not None:
        # THE DB-SOURCED TEXT'S DISCIPLINE (finding 14's cure — the
        # job-show seat's own face): error_class is written by the
        # ladder/reaper paths as a class name, but the COLUMN is text —
        # a hand-crafted or legacy row's value flows to the tty here
        # RAW (multi-line, ANSI-carrying, unbounded). The same
        # `_bounded_line` law the wf CLI's error-class seat carries
        # (collapse + the 60-class bound — one home, never a second
        # implementation).
        from taskq.workflows._cli import (
            _bounded_line,  # pyright: ignore[reportPrivateUsage]  # Why: the bound's ONE home is the wf CLI's own private — the discipline is imported, never duplicated.
        )

        typer.echo(f"error_class: {_bounded_line(row['error_class'], 60)}")
        # THE DB-SOURCED TEXT'S DISCIPLINE (the class seat: the job-show's
        # own face) — the collapse + the bound, never a raw multi-line or
        # escape-carrying blob in a line-oriented shell surface.
        typer.echo(f"error_message: {_format_event_detail(row['error_message'])}")
    if row["idempotency_key"] is not None:
        typer.echo(f"idempotency_key: {row['idempotency_key']}")
    if show_traceback:
        _print_blob_field("error_traceback", row["error_traceback"])
    if show_payload:
        _print_blob_field("payload", row["payload"])
    if archived:
        typer.echo("archived: yes")


def _print_blob_field(name: str, value: Any) -> None:
    """Print one opt-in blob field (``--traceback`` / ``--payload``).

    A ``(none)`` placeholder rather than silence: the operator asked this
    field by name, so an absent value must be distinguishable from a flag
    that did nothing. The value is printed in full and unbounded -- the
    flags exist to surface the blob, truncating it here would send the
    operator to SQL for the rest, the failure the admin UI's bounded
    render accepts for a page but a pointed ask does not need.
    """
    if value is None:
        typer.echo(f"{name}: (none)")
        return
    typer.echo(f"{name}:")
    for line in str(value).splitlines() or [""]:
        typer.echo(f"  {line}")


# ── job write path ─────────────────────────────────────────────────────
#
# The write commands (cancel, retry, cancel-where) talk to Postgres
# through the same Backend the admin UI's POST routes call, so a CLI
# cancel and an admin-UI cancel are one mechanism, not two write paths
# that can drift: write_cancel_request / retry_job / cancel_where carry
# the EPQ guards, the job_events writes, and the cooperative-cancel
# signalling. A short-lived TaskQ client (the public facade that owns
# the pool + PostgresBackend + JobsClient wiring) is the connection
# lifecycle here -- a bare asyncpg connection cannot run the backend's
# pooled drain machinery.


@contextlib.asynccontextmanager
async def _job_ops_client(settings: TaskQSettings) -> AsyncGenerator[TaskQ, None]:
    """Open a short-lived client for the job write commands, close it after.

    A whole client for one command is heavier than the read commands'
    single ``asyncpg.connect``: the write paths run on the backend, and
    the backend is pool-shaped (its drain batches on pooled connections,
    its NOTIFY targets a dispatcher pool). One command's lifetime bounds
    the cost; the pool stays at the client's small defaults.
    """
    tq = TaskQ(dsn=str(settings.pg_dsn), schema=settings.schema_name)
    await tq.open()
    try:
        yield tq
    finally:
        await tq.close()


@job_app.command("cancel")
def job_cancel(
    job_id: Annotated[str, typer.Argument(help="Job id (UUID).")],
    reason: Annotated[
        str | None,
        typer.Option("--reason", help="Recorded on the job's cancel_request event."),
    ] = None,
) -> None:
    """Cancel one job: pending/scheduled goes straight to terminal
    `cancelled`; running gets a cooperative cancel request the worker's
    cancel controller acts on."""
    settings = TaskQSettings.load()
    asyncio.run(_job_cancel(settings, job_id, reason))


async def _job_cancel(settings: TaskQSettings, job_id: str, reason: str | None) -> None:
    try:
        parsed = UUID(job_id)
    except ValueError:
        typer.echo(f"invalid job id (expected a UUID): {job_id!r}", err=True)
        raise typer.Exit(code=1) from None

    async with _job_ops_client(settings) as tq:
        # The admin route's pre-checks (web/admin/jobs.py job_cancel): a
        # missing id is a clean not-found, and a job already at rest is a
        # conflict named before any write -- a cancel that "succeeds"
        # against a terminal row would read as an action taken when the
        # write applied to nothing.
        row = await tq.get_row(JobId(parsed))
        if row is None:
            typer.echo(f"no job {parsed} in {settings.schema_name}.jobs", err=True)
            raise typer.Exit(code=1)
        if row.status in TERMINAL_STATUSES:
            typer.echo(
                f"job {parsed} is already in a terminal state (status: {row.status}); "
                "use `taskq job retry` to re-pend a resting job",
                err=True,
            )
            raise typer.Exit(code=1)
        result = await tq.cancel(JobId(parsed), reason)

    typer.echo(f"job: {result.job_id}")
    typer.echo(f"previous_status: {result.previous_status}")
    typer.echo(f"new_status: {result.new_status}")
    typer.echo(f"cancellation_initiated: {'yes' if result.cancellation_initiated else 'no'}")
    if result.new_status == "cancelled":
        typer.echo("outcome: cancelled directly (the job was not running)")
    elif result.cancellation_initiated:
        # The running row stays 'running' until the worker's heartbeat-
        # driven cancel controller observes cancel_phase=1 and the actor's
        # cancel_event produces the terminal write; the CLI cannot wait
        # for that (the actor owns the transition), so the honest print is
        # "requested", not "cancelled".
        typer.echo(
            "outcome: cooperative cancel requested (cancel_phase=1); the worker "
            "running the job will stop it at its next cancellation checkpoint"
        )
    else:
        typer.echo("outcome: no cancel was initiated (the job reached a terminal state first)")


@job_app.command("retry")
def job_retry(
    job_id: Annotated[str, typer.Argument(help="Job id (UUID).")],
) -> None:
    """Re-pend a resting job (every terminal status, including `succeeded`
    and `abandoned`). The attempt counter is not reset."""
    settings = TaskQSettings.load()
    asyncio.run(_job_retry(settings, job_id))


async def _job_retry(settings: TaskQSettings, job_id: str) -> None:
    try:
        parsed = UUID(job_id)
    except ValueError:
        typer.echo(f"invalid job id (expected a UUID): {job_id!r}", err=True)
        raise typer.Exit(code=1) from None

    async with _job_ops_client(settings) as tq:
        # The admin route's pre-checks (web/admin/ops.py job_retry): 404
        # on a missing row, 409 on a non-terminal status -- retrying a
        # running job would race that attempt's terminal write and can
        # execute the job twice concurrently, so the refusal is the
        # feature, not a limitation.
        row = await tq.get_row(JobId(parsed))
        if row is None:
            typer.echo(f"no job {parsed} in {settings.schema_name}.jobs", err=True)
            raise typer.Exit(code=1)
        if row.status not in TERMINAL_STATUSES:
            typer.echo(
                f"job {parsed} is not in a retryable state (status: {row.status}); "
                "a job is retryable only from a terminal status "
                "(succeeded, failed, cancelled, crashed, abandoned)",
                err=True,
            )
            raise typer.Exit(code=1)
        # The read above is only a pre-check: the write's own guard is the
        # arbiter (the admin route's wording). False means the row left a
        # retryable state between the two reads, or the attempt ceiling
        # cannot rise, so the write applied to nothing.
        retried = await tq.retry_job(JobId(parsed))
        if not retried:
            typer.echo(
                f"job {parsed} is not in a retryable state (it left a terminal status "
                "between the pre-check and the write, or its attempt ceiling cannot rise); "
                "no rows were changed",
                err=True,
            )
            raise typer.Exit(code=1)
        new_row = await tq.get_row(JobId(parsed))

    typer.echo(f"job: {parsed}")
    typer.echo(f"previous_status: {row.status}")
    typer.echo(f"new_status: {new_row.status if new_row is not None else '(gone)'}")
    typer.echo("outcome: re-pended (the attempt counter was not reset)")


def _parse_older_than(text: str) -> timedelta:
    """Parse a ``--older-than`` duration: ``45`` (seconds), ``30m``, ``2h``,
    ``7d``, ``2w``."""
    match = re.fullmatch(r"(\d+)([smhdw]?)", text.strip())
    if match is None:
        typer.echo(
            f"invalid --older-than duration: {text!r} (expected e.g. 45, 30m, 2h, 7d, 2w)",
            err=True,
        )
        raise typer.Exit(code=1)
    seconds = int(match.group(1))
    unit = match.group(2) or "s"
    unit_secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
    # A regex-valid duration can still outrun timedelta's own range
    # (999999999 days): the multiplication is exact, the constructor is
    # what overflows, and the raw OverflowError would surface as a
    # traceback instead of this function's clean one-line usage error.
    # The magnitude is rejected here, with the same shape of message the
    # non-matching form above emits, so junk is a usage error at the door
    # whichever way it fails the grammar.
    try:
        return timedelta(seconds=seconds * unit_secs)
    except OverflowError:
        typer.echo(
            f"invalid --older-than duration: {text!r} (the duration is too large)",
            err=True,
        )
        raise typer.Exit(code=1) from None


def _cancel_where_predicates_text() -> str:
    return "--queue, --status, --actor, --tag, or --older-than"


@job_app.command("cancel-where")
def job_cancel_where(
    queue: Annotated[
        str | None, typer.Option("--queue", help="Cancel only jobs on this queue.")
    ] = None,
    status: Annotated[
        list[str] | None,
        typer.Option(
            "--status",
            help="Cancel only jobs in this status. Repeatable. "
            "Valid: pending, scheduled, running, succeeded, failed, cancelled, "
            "crashed, abandoned.",
        ),
    ] = None,
    actor: Annotated[
        str | None, typer.Option("--actor", help="Cancel only jobs for this actor name.")
    ] = None,
    tag: Annotated[
        list[str] | None,
        typer.Option("--tag", help="Cancel only jobs carrying this tag. Repeatable."),
    ] = None,
    older_than: Annotated[
        str | None,
        typer.Option(
            "--older-than",
            help="Only jobs enqueued before now minus this duration (e.g. 45, 30m, 2h, 7d, 2w).",
        ),
    ] = None,
    reason: Annotated[
        str | None,
        typer.Option("--reason", help="Recorded on every matched job's cancel_request event."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print the matching count and sample ids; write nothing."),
    ] = False,
) -> None:
    """Bulk cancel every job matching the given filters.

    Refuses to run with no filter at all: a naked full-table cancel is
    the one mistake this command cannot walk back.
    """
    settings = TaskQSettings.load()
    asyncio.run(
        _job_cancel_where(
            settings,
            queue=queue,
            status=tuple(status) if status else None,
            actor=actor,
            tags=tuple(tag) if tag else None,
            older_than=older_than,
            reason=reason,
            dry_run=dry_run,
        )
    )


async def _job_cancel_where(
    settings: TaskQSettings,
    *,
    queue: str | None,
    status: tuple[str, ...] | None,
    actor: str | None,
    tags: tuple[str, ...] | None,
    older_than: str | None,
    reason: str | None,
    dry_run: bool,
) -> None:
    # The empty-filter guard fires HERE, before any connection is opened,
    # with the backend's EmptyFilterError as the semantic twin: the client
    # layer raises the same refusal a call further down would, but failing
    # before the pool exists keeps the loud guardrail from costing a
    # round trip, and the CLI offers no allow_empty_filter bypass -- a
    # shell one-liner is exactly the context a naked full-table cancel
    # comes from.
    created_before: datetime | None = None
    if older_than is not None:
        # The cutoff comes from this process's clock while created_at is
        # stamped by the database's; the skew that mixes is NTP-scale, and
        # --dry-run re-derives the same cutoff, so the preview answers for
        # the write that follows it.
        created_before = datetime.now(UTC) - _parse_older_than(older_than)

    try:
        job_filter = JobFilter(
            queue=queue,
            status=status,  # pyright: ignore[reportArgumentType]  # Why: validated one step down; JobFilter.__post_init__ rejects unknown statuses with the full valid list.
            actor=actor,
            tags=tags,
            created_before=created_before,
        )
    except ValueError as exc:
        # Unknown --status values land here, named with the valid set, the
        # same failure the backend's filter validation raises.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    if not job_filter.has_predicates():
        typer.echo(
            "cancel-where requires at least one filter predicate "
            f"({_cancel_where_predicates_text()}); a filter with no predicates "
            "would cancel the entire table. The CLI offers no bypass for this "
            "guardrail.",
            err=True,
        )
        raise typer.Exit(code=1)

    if dry_run:
        await _cancel_where_dry_run(settings, job_filter)
        return

    try:
        result = await _cancel_where_write(settings, job_filter, reason)
    except EmptyFilterError as exc:
        # Unreachable via this command (the guard above runs first), but
        # caught rather than trusted: the backend's guardrail is the
        # authority on what an empty filter is, and the CLI must surface
        # its refusal, not assume its own copy stays in sync.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None

    typer.echo(f"cancelled directly: {result.cancelled_directly} (pending/scheduled -> cancelled)")
    typer.echo(
        f"cooperative cancel requested: {result.cancel_requested} (running -> cancel_phase=1, "
        "the workers running them will stop them at their next cancellation checkpoint)"
    )
    typer.echo(f"total affected: {result.total_affected}")
    for label, ids in (
        ("cancelled", result.cancelled_ids),
        ("cancel requested", result.cancel_requested_ids),
    ):
        sample = ids[:5]
        display = ", ".join(str(i) for i in sample)
        if len(ids) > len(sample):
            display += f", and {len(ids) - len(sample)} more"
        if display:
            typer.echo(f"{label}: {display}")


async def _cancel_where_write(
    settings: TaskQSettings, job_filter: JobFilter, reason: str | None
) -> BulkCancelResult:
    async with _job_ops_client(settings) as tq:
        return await tq.cancel_where(job_filter, reason)


async def _cancel_where_dry_run(settings: TaskQSettings, job_filter: JobFilter) -> None:
    """Print the matching count and a few sample ids, write nothing.

    The conditions come from ``build_filter_conditions`` -- the same
    builder ``cancel_where`` itself will apply -- so the preview can
    never describe a match set the write disagrees with.
    """
    if not _IDENT_RE.match(settings.schema_name):
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)
    filter_sql = build_filter_conditions(job_filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    count_sql = (
        "WITH matching AS MATERIALIZED ( "  # noqa: S608  # Why: schema is identifier-validated above; the conditions are the shared bound-parameter filter builder's output, every value bound by asyncpg.
        f'SELECT id, created_at FROM "{settings.schema_name}".jobs '
        f"WHERE {conditions_str}"
        " ) "
        "SELECT (SELECT count(*)::int FROM matching) AS total, "
        "(SELECT array_agg(id ORDER BY created_at DESC, id) "
        "FROM (SELECT id, created_at FROM matching "
        "ORDER BY created_at DESC, id LIMIT 5) sample) AS sample_ids"
    )
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        row = await conn.fetchrow(count_sql, *filter_sql.params)
    finally:
        await close_conn_bounded(conn, "job-cancel-where-dry-run", CLOSE_TIMEOUT_SECS)
    total = int(row["total"]) if row is not None else 0
    sample_ids: list[UUID] = list(row["sample_ids"] or []) if row is not None else []
    typer.echo(f"dry run (nothing written): {total} matching job(s)")
    for job_id in sample_ids:
        typer.echo(f"  {job_id}")
    if total > len(sample_ids):
        typer.echo(f"  ... and {total - len(sample_ids)} more")


@job_app.command("events")
def job_events(
    job_id: Annotated[str, typer.Argument(help="Job id (UUID).")],
) -> None:
    """List one job's job_events timeline (kind, occurred_at, detail)."""
    settings = TaskQSettings.load()
    asyncio.run(_job_events(settings, job_id))


async def _job_events(settings: TaskQSettings, job_id: str) -> None:
    try:
        parsed = UUID(job_id)
    except ValueError:
        typer.echo(f"invalid job id (expected a UUID): {job_id!r}", err=True)
        raise typer.Exit(code=1) from None
    if not _IDENT_RE.match(settings.schema_name):
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)

    conn = await asyncpg.connect(str(settings.pg_dsn))
    archived = False
    try:
        # Existence probe first (the show command's jobs -> jobs_archive
        # fallback): an unknown id must read as "no such job", not as the
        # empty timeline a bare events query would return for it.
        exists = await conn.fetchval(
            f'SELECT EXISTS (SELECT 1 FROM "{settings.schema_name}".jobs WHERE id = $1) '  # noqa: S608  # Why: schema is identifier-validated above; asyncpg cannot bind identifiers, the id is a bound parameter.
            f'OR EXISTS (SELECT 1 FROM "{settings.schema_name}".jobs_archive WHERE id = $1)',
            parsed,
        )
        if not exists:
            typer.echo(
                f"no job {parsed} in {settings.schema_name}.jobs or "
                f"{settings.schema_name}.jobs_archive",
                err=True,
            )
            raise typer.Exit(code=1)
        rows = await conn.fetch(
            f'SELECT occurred_at, kind, detail FROM "{settings.schema_name}".job_events '  # noqa: S608  # Why: schema is identifier-validated above; asyncpg cannot bind identifiers, the job id is a bound parameter.
            "WHERE job_id = $1 ORDER BY occurred_at, id",
            parsed,
        )
        if not rows:
            archived_row = await conn.fetchval(
                f'SELECT EXISTS (SELECT 1 FROM "{settings.schema_name}".jobs_archive WHERE id = $1)',  # noqa: S608  # Why: schema is identifier-validated above.
                parsed,
            )
            archived = bool(archived_row)
    finally:
        await close_conn_bounded(conn, "job-events", CLOSE_TIMEOUT_SECS)

    if not rows:
        if archived:
            typer.echo(f"job {parsed} (archived) has no job_events rows")
        else:
            typer.echo(f"job {parsed} has no job_events rows")
        return
    for event in rows:
        detail = _format_event_detail(event["detail"])
        typer.echo(f"{event['occurred_at']}  {event['kind']}  {detail}")


_EVENT_DETAIL_LINE_LIMIT: Final[int] = 120
"""One-line cap for a ``job events`` detail render. The detail jsonb is
free-form; a timeline read must stay a timeline, so a long detail is
truncated with the dropped-character count named, the admin UI's
traceback-truncation convention at timeline scale."""


def _format_event_detail(detail: Any) -> str:
    """Render an event's detail jsonb as one bounded line."""
    if detail is None:
        return ""
    text = " ".join(str(detail).split())
    if len(text) <= _EVENT_DETAIL_LINE_LIMIT:
        return text
    remaining = len(text) - _EVENT_DETAIL_LINE_LIMIT
    suffix = f"... (+{remaining} characters)"
    return text[: _EVENT_DETAIL_LINE_LIMIT - len(suffix)] + suffix


# ── flows: the workflow-run surface (T12) ───────────────────────────────
#
# One question, one command: stuck → status, deliver → signal, reply →
# resolve, stop → cancel, retry-a-failed-node → retry, inventory → list.
# The analysis lives in taskq.workflows._cli (the _doctor seam); the
# write verbs call the engine's module-level run-operator functions — the
# same functions the FlowRunner methods delegate to, never a second write
# path. Every write rides its engine-owned audit row (the same-tx
# guarantee); the CLI names its principal explicitly (``cli:<user>``) —
# "who did this" is a ROW for every surface, the shell included.

_FLOWS_LIST_LIMIT: Final[int] = 20


def _cli_principal() -> str:
    """The CLI's audit subject: THE KERNEL'S WORD (F-CLI-3's cure) —
    ``os.getuid()`` through ``pwd``, never the environment. ``getpass.
    getuser()`` reads ``LOGNAME``/``USER`` first, so
    ``LOGNAME=postgres taskq flows resolve …`` wrote the mutation as
    ``cli:postgres`` — the audit row's principal was spoofable by any
    env var the calling shell already controls. The uid is the one
    fact the kernel attests; an unresolvable uid (a container without
    the passwd entry) names the NUMBER, never a borrowed name."""
    import os

    try:
        import pwd

        name = pwd.getpwuid(os.getuid()).pw_name
    except (ImportError, KeyError):
        name = f"uid{os.getuid()}"
    return f"cli:{name}"


def _flows_guard(settings: TaskQSettings) -> None:
    """The shared pre-flight: the schema identifier re-checked at the
    interpolation site (the job-show convention). Prints the reason and
    exits 1 — never a traceback."""
    if not _IDENT_RE.match(settings.schema_name):
        typer.echo(f"invalid schema name: {settings.schema_name!r}", err=True)
        raise typer.Exit(code=1)


def _parse_run_id(run_id: str) -> UUID:
    """The run id's parse (the guard's other half): a non-UUID is the
    named error + exit 1."""
    try:
        return UUID(run_id)
    except ValueError:
        typer.echo(f"invalid run id (expected a UUID): {run_id!r}", err=True)
        raise typer.Exit(code=1) from None


@contextlib.asynccontextmanager
async def _flows_pool(settings: TaskQSettings) -> AsyncGenerator[asyncpg.Pool, None]:
    """A short-lived pool for one flows command (the job write path's
    lifecycle convention — the engine's operators are pool-shaped)."""
    _flows_guard(settings)
    pool = await asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=2)
    try:
        yield pool
    finally:
        await close_pool_bounded(pool, "flows", CLOSE_TIMEOUT_SECS)


@flows_app.command("list")
def flows_list(
    limit: Annotated[
        int, typer.Option("--limit", help="How many recent runs to show.")
    ] = _FLOWS_LIST_LIMIT,
) -> None:
    """Which runs exist, newest first, with their derived statuses."""
    if limit < 1:
        # TRIO-3: a negative/zero limit is a NAMED refusal (the SQL's
        # LIMIT clause rejects it as a bare InvalidRowCountInLimitClauseError
        # traceback otherwise).
        typer.echo(f"invalid --limit {limit}: the limit counts runs and must be >= 1", err=True)
        raise typer.Exit(code=1)
    settings = TaskQSettings.load()
    asyncio.run(_flows_list(settings, limit))


async def _flows_list(settings: TaskQSettings, limit: int) -> None:
    from taskq.workflows._cli import (
        FLOW_ROOT_STEP_KEY,
        FlowListRow,
        FlowNodeRow,
        derive_flow_status,
        format_flow_list,
    )

    conn = await asyncpg.connect(_dsn_of(settings))
    try:
        rows = await conn.fetch(
            f"""
            WITH runs AS (
                SELECT id, actor, status, created_at, metadata->>'workflow' AS workflow
                FROM "{settings.schema_name}".jobs
                WHERE step_key = '{FLOW_ROOT_STEP_KEY}'
                ORDER BY created_at DESC
                LIMIT $1
            )
            SELECT r.id, r.workflow, r.status AS root_status, r.created_at,
                   n.status AS node_status, n.deps_pending,
                   n.metadata->>'blocking_reason' AS blocking_reason,
                   n.step_key,
                   EXISTS (
                       SELECT 1
                       FROM "{settings.schema_name}".wf_edge e
                       JOIN "{settings.schema_name}".jobs j2 ON j2.id = e.child_id
                       WHERE e.parent_id = n.id
                         AND e.failure_policy IN ('collect', 'maybe')
                         AND NOT (j2.status = 'pending' AND j2.metadata->>'blocking_reason' IN ('failed_parent', 'orphan_parent', 'flow_dead'))
                   ) AS absorbed,
                   s.id AS hold_id, s.signal_name, s.expires_at AS hold_expires_at
            FROM runs r
            LEFT JOIN "{settings.schema_name}".jobs n
              ON (n.metadata->>'flow_id')::uuid = r.id
             AND n.metadata ? 'flow_id'
             AND n.step_key <> '{FLOW_ROOT_STEP_KEY}'
            LEFT JOIN "{settings.schema_name}".wf_signals s
              ON s.workflow_id = r.id AND s.status = 'held' AND s.node_key = n.step_key
            ORDER BY r.created_at DESC, r.id, n.id
            """,  # noqa: S608  # Why: only the identifier-validated schema and the constant root key interpolate; the limit is a bound parameter.
            limit,
        )
    except asyncpg.exceptions.UndefinedTableError:
        # TRIO-1: the schema was never migrated — the named remedy, never
        # a traceback.
        typer.echo(
            f"the schema {settings.schema_name!r} has no workflow tables yet — "
            "the surface needs the migrations: run `taskq migrate` against this "
            "database, then retry",
            err=True,
        )
        raise typer.Exit(code=1) from None
    finally:
        await close_conn_bounded(conn, "flows-list", CLOSE_TIMEOUT_SECS)

    by_run: dict[Any, dict[str, Any]] = {}
    for row in rows:
        entry = by_run.setdefault(
            row["id"],
            {
                "workflow": row["workflow"] or row["actor"],
                "root_status": row["root_status"],
                "created_at": row["created_at"],
                "nodes": [],
            },
        )
        if row["node_status"] is not None:
            hold = None
            if row["hold_id"] is not None:
                from taskq.workflows.api._hitl import HoldContext

                hold = HoldContext(
                    hold_id=str(row["hold_id"]),
                    run_id=str(row["id"]),
                    node_key=row["step_key"],
                    signal_name=row["signal_name"],
                    hold_epoch=0,
                    call_id="",
                    payload={},
                    payload_schema=None,
                    reason=None,
                    created_at=None,
                    expires_at=row["hold_expires_at"],
                    status="held",
                )
            entry["nodes"].append(
                FlowNodeRow(
                    step_key="",
                    status=row["node_status"],
                    deps_pending=row["deps_pending"],
                    blocking_reason=row["blocking_reason"],
                    absorbed=row["absorbed"] or False,
                    hold=hold,
                )
            )
    list_rows = [
        FlowListRow(
            run_id=str(run_id),
            workflow=entry["workflow"],
            root_status=entry["root_status"],
            derived=derive_flow_status(entry["nodes"]),
            nodes_total=len(entry["nodes"]),
            created_at=entry["created_at"],
        )
        for run_id, entry in by_run.items()
    ]
    for line in format_flow_list(list_rows):
        typer.echo(line)


def _dsn_of(settings: TaskQSettings) -> str:
    """The read verbs' DSN, after the schema guard (the job-show
    convention)."""
    _flows_guard(settings)
    return str(settings.pg_dsn)


@flows_app.command("status")
def flows_status(
    run_id: Annotated[str, typer.Argument(help="The workflow run's id (UUID).")],
) -> None:
    """What happened / where is it blocked / what happens next."""
    settings = TaskQSettings.load()
    asyncio.run(_flows_status(settings, run_id))


async def _flows_status(settings: TaskQSettings, run_id: str) -> None:
    from taskq.workflows._cli import FlowNodeRow, format_flow_status
    from taskq.workflows._sql import WorkflowSql

    _flows_guard(settings)
    parsed = _parse_run_id(run_id)
    wsql = WorkflowSql.build(settings.schema_name)
    conn = await asyncpg.connect(_dsn_of(settings))
    try:
        root = await conn.fetchrow(
            f"SELECT status, actor, metadata->>'workflow' AS workflow, "  # noqa: S608  # Why: schema identifier-validated above.
            "cancel_requested_at FROM "
            f"\"{settings.schema_name}\".jobs WHERE id = $1 AND step_key = '__flow__'",
            parsed,
        )
        if root is None:
            typer.echo(f"no run {parsed} (source: jobs where step_key = '__flow__')", err=True)
            typer.echo("Action: taskq flows list shows the recent runs.", err=True)
            raise typer.Exit(code=1)
        node_rows = await conn.fetch(wsql.workflow_nodes, parsed)
        held = await conn.fetch(
            f"SELECT id, node_key, signal_name, hold_epoch, payload, payload_schema, "  # noqa: S608  # Why: schema identifier-validated above.
            "created_at, expires_at FROM "
            f'"{settings.schema_name}".wf_signals '
            "WHERE workflow_id = $1 AND status = 'held'",
            parsed,
        )
    finally:
        await close_conn_bounded(conn, "flows-status", CLOSE_TIMEOUT_SECS)

    holds_by_key = {row["node_key"]: row for row in held}
    nodes = [
        FlowNodeRow(
            step_key=row["step_key"],
            status=row["status"],
            deps_pending=row["deps_pending"],
            blocking_reason=row["blocking_reason"],
            absorbed=row["absorbed"],
            error_class=row["error_class"],
            error_message=row["error_message"],
            # THE ATTEMPT COUNTERS RIDE THE READ (attack-4
            # F-P4-WHYSTUCK-LADDER-LIE's cure): the dataclass defaults
            # (0 / 3) made every failed row report "ladder headroom 3" —
            # the read must carry the row's own counters.
            max_attempts=row["max_attempts"],
            attempt=row["attempt"],
            hold=(
                _held_context(run_id, holds_by_key[row["step_key"]])
                if row["step_key"] in holds_by_key
                else None
            ),
        )
        for row in node_rows
    ]
    for line in format_flow_status(
        run_id=run_id,
        workflow=root["workflow"] or root["actor"],
        root_status=root["status"],
        nodes=nodes,
        cancel_in_flight=root["cancel_requested_at"] is not None,
    ):
        typer.echo(line)


def _held_context(run_id: str, signal_row: Any) -> Any:
    """The held node's context from the JOINED signal row (the status
    read is one round trip; the context's fields are the HoldContext
    contract's — the payload's reason/tool/args ride the row)."""
    from taskq.workflows.api._hitl import HoldContext

    payload: object = signal_row["payload"]
    if isinstance(payload, str):
        payload = json_loads(payload)
    # The row's jsonb decodes to a json dict; the reason/tool/args keys
    # ride INSIDE it until the deliver overwrites the payload (T10's
    # deliver-no-drop shape).
    doc = cast(dict[str, object], payload) if isinstance(payload, dict) else {}
    raw_reason = doc.get("reason")
    reason = raw_reason if isinstance(raw_reason, str) else None
    return HoldContext(
        hold_id=str(signal_row["id"]),
        run_id=run_id,
        node_key=signal_row["node_key"],
        signal_name=signal_row["signal_name"],
        hold_epoch=signal_row["hold_epoch"],
        call_id="",
        payload=payload,
        payload_schema=signal_row["payload_schema"],
        reason=reason,
        created_at=signal_row["created_at"],
        expires_at=signal_row["expires_at"],
        status="held",
    )


@flows_app.command("holds")
def flows_holds(
    run_id: Annotated[str, typer.Argument(help="The workflow run's id (UUID).")],
) -> None:
    """What is this run waiting on (the pending HITL holds)."""
    from taskq.workflows._cli import format_holds
    from taskq.workflows.api._hitl import HitlClient

    settings = TaskQSettings.load()
    _flows_guard(settings)
    parsed = _parse_run_id(run_id)

    async def run() -> None:
        async with _flows_pool(settings) as pool:
            try:
                holds = await HitlClient(pool, schema=settings.schema_name).list(str(parsed))
            except asyncpg.exceptions.UndefinedTableError:
                # TRIO-2: the schema never grew the workflow tables — the
                # named remedy, never the traceback.
                typer.echo(
                    f"the schema {settings.schema_name!r} has no wf_signals table yet — "
                    "the holds surface needs the migrations: run `taskq migrate` "
                    "against this database, then retry",
                    err=True,
                )
                raise typer.Exit(code=1) from None
            for line in format_holds(holds, run_id=run_id):
                typer.echo(line)

    asyncio.run(run())


def _load_flows_app(app_ref: str) -> Any:
    """The typed door's source: the operator's WorkflowApp module
    (``--app myapp.workflows:app``), loaded through the shared
    ``module:attr`` resolver."""
    app_obj = _import_ref(app_ref, example="myapp.workflows:app")
    if not hasattr(app_obj, "get"):
        typer.echo(
            f"--app must name a WorkflowApp (a module:attr whose object has "
            f"`.get(workflow)`); {app_ref} does not",
            err=True,
        )
        raise typer.Exit(code=1)
    return app_obj


def _validate_through_gate(
    app_obj: Any,
    workflow: str,
    node_key: str,
    signal_name: str,
    payload: dict[str, object],
) -> dict[str, object]:
    """THE TYPED DOOR (T09's bound gates; the hitl-proof's send
    boundary): the shell's JSON payload re-validates against the gate's
    declared models BEFORE any row moves. A wrong payload answers the
    NAMED pydantic error and nothing is delivered — no untyped deliver
    surface ships."""
    from pydantic import ValidationError

    from taskq.workflows._cli import gate_models_for
    from taskq.workflows.api._validate import WorkflowValidationError

    try:
        models = gate_models_for(app_obj, workflow, node_key)
    except KeyError as exc:
        # THE NAMED REFUSAL (attack-4 F-P4-CLI-KEYERROR-TRACEBACK's cure):
        # the stale-deploy world — the run's workflow is not declared on
        # THIS app (the --app module predates the run, or the node key
        # does not exist on the declared workflow). The admin's twin
        # catches KeyError; the CLI's contract is the same: the NAMED
        # error + exit 1, never a rich traceback.
        typer.echo(
            f"{exc.args[0] if exc.args else exc!r} — the typed door cannot "
            f"validate signal {signal_name!r} against this app; pass the "
            "--app module that declares the run's workflow (the stale-"
            "deploy shape: the app moved since the run started)",
            err=True,
        )
        raise typer.Exit(code=1) from None
    except WorkflowValidationError as exc:
        # THE DOOR'S OWN REPORT IS THE REFUSAL (the registration door's
        # CLI face — found by THIS lane's attack round: app.get() validates
        # since the F-LOOP-7 cure, so an INVALID graph's compile error
        # escaped the typed door as a rich traceback — the diagnostics-
        # first law's own violation). The validation report NAMES the rule
        # + the fix; the operator's terminal carries it, exit 1.
        typer.echo(
            f"the run's workflow {workflow!r} does not validate on this "
            f"app — {exc} (fix the definition and redeploy; the typed "
            "door cannot resolve its gates on an invalid graph)",
            err=True,
        )
        raise typer.Exit(code=1) from None
    if not models:
        typer.echo(
            f"node {node_key!r} of workflow {workflow!r} declares no gate — "
            f"signal {signal_name!r} cannot be delivered through it",
            err=True,
        )
        raise typer.Exit(code=1)
    members = signal_name.split("|")
    candidates = [m for m in models if m.__name__ in members]
    if not candidates:
        typer.echo(
            f"the hold declares signal {signal_name!r} but the node's bound gates are "
            f"{[m.__name__ for m in models]} — the shapes cannot match",
            err=True,
        )
        raise typer.Exit(code=1)
    errors: list[str] = []
    for model in candidates:
        try:
            validated = model.model_validate(payload)
        except ValidationError as exc:
            errors.append(
                f"{model.__name__}: "
                + "; ".join(
                    f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors()
                )
            )
            continue
        dumped = validated.model_dump(mode="json")
        return dict(dumped)  # type: ignore[arg-type]  # Why: model_dump(mode='json') returns dict[str, Any]; the gate's models are object-valued pydantics and the delivered payload is the row's jsonb.
    typer.echo(
        f"pydantic refused the payload for signal {signal_name!r}: " + " | ".join(errors),
        err=True,
    )
    raise typer.Exit(code=1)


async def _workflow_name_of(pool: asyncpg.Pool, schema: str, run_id: UUID) -> tuple[str, str, str]:
    """The run's (workflow name, node_key-source, root status) — the
    minimal root read the write verbs need to address the typed door."""
    root = await pool.fetchval(
        f"SELECT COALESCE(metadata->>'workflow', actor) FROM \"{schema}\".jobs "  # noqa: S608  # Why: schema identifier-validated by the caller's guard.
        "WHERE id = $1 AND step_key = '__flow__'",
        run_id,
    )
    if root is None:
        typer.echo(f"no run {run_id} (source: jobs where step_key = '__flow__')", err=True)
        typer.echo("Action: taskq flows list shows the recent runs.", err=True)
        raise typer.Exit(code=1)
    return str(root), "", ""


@flows_app.command("signal")
def flows_signal(
    run_id: Annotated[str, typer.Argument(help="The workflow run's id (UUID).")],
    node: Annotated[str, typer.Argument(help="The node key that holds.")],
    payload: Annotated[str, typer.Argument(help="The payload, as a JSON object.")],
    app_ref: Annotated[
        str,
        typer.Option(
            "--app",
            help="Module:attr reference to the WorkflowApp (e.g. myapp.workflows:app) — "
            "the bound gates' source; the payload validates through them.",
        ),
    ],
    reason: Annotated[
        str | None, typer.Option("--reason", help="Why — recorded on the audit row.")
    ] = None,
) -> None:
    """Deliver a typed signal to a held node (by run + node)."""
    from taskq.workflows._cli import parse_decision

    settings = TaskQSettings.load()
    _flows_guard(settings)
    parsed = _parse_run_id(run_id)
    try:
        decision = parse_decision(payload)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    app_obj = _load_flows_app(app_ref)
    asyncio.run(_flows_signal(settings, parsed, node, decision, app_obj, reason))


async def _flows_signal(
    settings: TaskQSettings,
    run_id: UUID,
    node: str,
    decision: dict[str, object],
    app_obj: Any,
    reason: str | None,
) -> None:
    from taskq.workflows.api._hitl import HitlClient

    async with _flows_pool(settings) as pool:
        client = HitlClient(pool, schema=settings.schema_name)
        held = [h for h in await client.list(str(run_id)) if h.node_key == node]
        if not held:
            typer.echo(
                f"node {node!r} of run {run_id} holds nothing "
                "(source: wf_signals rows with status = 'held')",
                err=True,
            )
            typer.echo("Action: taskq flows holds shows what IS pending.", err=True)
            raise typer.Exit(code=1)
        if len(held) > 1:
            typer.echo(
                f"node {node!r} holds {len(held)} signals "
                f"({', '.join(h.signal_name for h in held)}) — address one BY ID: "
                "taskq flows resolve <hold_id> <decision>",
                err=True,
            )
            raise typer.Exit(code=1)
        hold = held[0]
        workflow = (await _workflow_name_of(pool, settings.schema_name, run_id))[0]
        validated = _validate_through_gate(app_obj, workflow, node, hold.signal_name, decision)
        result = await client.resolve(
            hold.hold_id, validated, reason=reason, principal=_cli_principal()
        )
        _echo_delivery(result, hold.hold_id)


def _echo_delivery(result: Any, hold_id: str) -> None:
    """The DeliveryResult's one rendering (the exit-code contract:
    delivered → 0, no-op → 0 with the note, refused → 1)."""
    from taskq.workflows.api._hitl import DeliveryResult

    assert isinstance(result, DeliveryResult)
    if result.status == "delivered":
        typer.echo(f"delivered: hold {hold_id} — the node resumes on its next drive")
    elif result.status == "no-op":
        typer.echo(f"no-op: hold {hold_id} — {result.reason}")
    else:
        typer.echo(f"refused: {result.reason}", err=True)
        raise typer.Exit(code=1)


@flows_app.command("resolve")
def flows_resolve(
    hold_id: Annotated[str, typer.Argument(help="The hold's id (the reply handle).")],
    decision: Annotated[str, typer.Argument(help="The decision, as a JSON object.")],
    app_ref: Annotated[
        str,
        typer.Option(
            "--app",
            help="Module:attr reference to the WorkflowApp (e.g. myapp.workflows:app) — "
            "the bound gates' source; the decision validates through them.",
        ),
    ],
    reason: Annotated[
        str | None, typer.Option("--reason", help="Why — recorded on the audit row.")
    ] = None,
) -> None:
    """Reply to a hold BY ID (the reply handle every surface shows)."""
    from taskq.workflows._cli import parse_decision

    settings = TaskQSettings.load()
    try:
        parsed = UUID(hold_id)
    except ValueError:
        typer.echo(f"invalid hold id (expected a UUID): {hold_id!r}", err=True)
        raise typer.Exit(code=1) from None
    try:
        decision_dict = parse_decision(decision)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    app_obj = _load_flows_app(app_ref)
    asyncio.run(_flows_resolve(settings, parsed, decision_dict, app_obj, reason))


async def _flows_resolve(
    settings: TaskQSettings,
    hold_id: UUID,
    decision: dict[str, object],
    app_obj: Any,
    reason: str | None,
) -> None:
    from taskq.workflows.api._hitl import HitlClient

    async with _flows_pool(settings) as pool:
        client = HitlClient(pool, schema=settings.schema_name)
        hold = await client.get(str(hold_id))
        if hold is None:
            typer.echo(
                f"no hold {hold_id} (source: wf_signals by id) — the reply handle is "
                "on every surface that shows the hold",
                err=True,
            )
            raise typer.Exit(code=1)
        root_name = (await _workflow_name_of(pool, settings.schema_name, UUID(hold.run_id)))[0]
        validated = _validate_through_gate(
            app_obj, root_name, hold.node_key, hold.signal_name, decision
        )
        try:
            result = await client.resolve(
                str(hold_id), validated, reason=reason, principal=_cli_principal()
            )
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            # THE RACE'S HONEST LOSER (the same deadlock-refusal the cancel
            # verb carries — the two verbs' cascades can deadlock; PG kills
            # one writer; the CLI's answer is the NAMED refusal with the
            # remedy, never a rich traceback — the diagnostics-first law).
            typer.echo(
                f"the hold {hold_id}'s writers raced and PG resolved the "
                "deadlock in another writer's favour — the hold is being "
                "resolved OR the run cancelled by the winner; re-run this "
                "verb (or `taskq flows status`) to see the settled state",
                err=True,
            )
            raise typer.Exit(code=1) from None
        _echo_delivery(result, str(hold_id))


@flows_app.command("cancel")
def flows_cancel(
    run_id: Annotated[str, typer.Argument(help="The workflow run's id (UUID).")],
    reason: Annotated[
        str | None, typer.Option("--reason", help="Why — recorded on the audit row.")
    ] = None,
) -> None:
    """Stop a workflow run (the engine's cancel cascade)."""
    from taskq.workflows import cancel_workflow_run

    settings = TaskQSettings.load()
    _flows_guard(settings)
    parsed = _parse_run_id(run_id)

    async def run() -> None:
        async with _flows_pool(settings) as pool:
            # F-CLI-2: the GHOST distinguished from the terminal — a run id
            # that exists in NO table is the honest rc=1 refusal (the same
            # answer `flows status` gives), never a silent "no-op" with
            # rc=0 over an id that names nothing.
            exists = await pool.fetchval(
                f'SELECT 1 FROM "{settings.schema_name}".jobs '  # noqa: S608  # Why: schema identifier-validated above.
                "WHERE id = $1 AND step_key = '__flow__'",
                parsed,
            )
            if exists is None:
                typer.echo(
                    f"no run {parsed} (source: jobs where step_key = '__flow__') — "
                    "nothing to cancel",
                    err=True,
                )
                raise typer.Exit(code=1)
            stopped = None
            try:
                stopped = await cancel_workflow_run(
                    pool,
                    schema=settings.schema_name,
                    flow_id=JobId(parsed),
                    reason=reason,
                    principal=_cli_principal(),
                )
            except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
                # THE RACE'S HONEST LOSER (the resolve/cancel race pin's
                # conviction — the two verbs' cascades can deadlock: PG
                # kills one writer, and the CLI's answer must be the NAMED
                # refusal with the remedy, never a rich traceback — the
                # diagnostics-first law). The winner's own voice is honest;
                # the loser retries by re-running the verb.
                typer.echo(
                    f"the run {parsed}'s writers raced and PG resolved the "
                    "deadlock in another writer's favour — the run is being "
                    "cancelled OR resolved by the winner; re-run this verb "
                    "to see the settled state",
                    err=True,
                )
                raise typer.Exit(code=1) from None
            if stopped:
                typer.echo(
                    f"cancelled: run {parsed} (the cascade landed; "
                    f"{stopped - 1} held signal(s) resolved)"
                )
            else:
                typer.echo(f"no-op: run {parsed} is already terminal — nothing cancelled")

    asyncio.run(run())


@flows_app.command("retry")
def flows_retry(
    run_id: Annotated[str, typer.Argument(help="The workflow run's id (UUID).")],
    node: Annotated[str, typer.Argument(help="The node key to re-run.")],
    reason: Annotated[
        str | None, typer.Option("--reason", help="Why — recorded on the audit row.")
    ] = None,
) -> None:
    """Re-run a failed node and re-open its blocked closure."""
    from taskq.workflows import retry_workflow_node

    settings = TaskQSettings.load()
    _flows_guard(settings)
    parsed = _parse_run_id(run_id)

    async def run() -> None:
        from taskq.workflows import WorkflowRunError

        try:
            async with _flows_pool(settings) as pool:
                reopened = await retry_workflow_node(
                    pool,
                    schema=settings.schema_name,
                    flow_id=JobId(parsed),
                    node_key=node,
                    reason=reason,
                    principal=_cli_principal(),
                )
        except WorkflowRunError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from None
        if reopened:
            typer.echo(
                f"re-opened {reopened} row(s): node {node!r} re-pends, its blocked "
                "closure clears — the run resumes on its next drive"
            )
        else:
            typer.echo(
                f"nothing re-opened: node {node!r} is live, unknown to run {parsed}, "
                "or spent past the attempt ceiling",
                err=True,
            )
            typer.echo("Action: taskq flows status shows the node's current state.", err=True)
            raise typer.Exit(code=1)

    asyncio.run(run())


if __name__ == "__main__":
    main()
