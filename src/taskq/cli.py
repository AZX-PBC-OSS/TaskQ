"""``taskq`` CLI entry point.

Usage::

    taskq migrate status
    taskq migrate up [--phase pre|post] [--target VERSION] [--max-steps N]
    taskq worker --actors myapp.actors:registry
    taskq job show JOB_ID

The console script puts the current working directory on ``sys.path``
(see :func:`main`), so ``module:attr`` options resolve application modules
from the directory the operator ran the command in.
"""

import asyncio
import contextlib
import importlib
import os
import sys
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from datetime import timedelta
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
    close_redis_bounded,
)
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
    build_worker_connections,
    make_dedicated_conn_factory,
    make_pg_pool_factory,
    make_redis_client_factory,
)
from taskq.backend._protocol import parse_retry_kind
from taskq.connections import ConnFactory, PoolFactory, RedisFactory, WorkerConnections
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex for defence-in-depth schema validation at this SQL interpolation site, the queue_ops convention.
)
from taskq.exceptions import ActorConfigDriftList, ActorDeregistrationError, ActorNotFoundError
from taskq.obs import OtelExporterConfigurationError, configure_exporters, setup_logging
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.worker.dev import dev_watch_loop
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
# dispatcher_command_timeout), but `taskq ui serve` loads TaskQSettings —
# the base class carries none of them. The literals mirror those defaults
# exactly; module-level so tests shrink them as seams (the
# CLOSE_TIMEOUT_SECS convention).
_UI_FACTORY_TIMEOUT_SECS: Final[float] = 30.0
"""Bounds the UI's first-use awaits — ``pool_factory()`` /
``redis_factory()`` (the AAD first token fetch lives inside them) and the
eager redis ``initialize()`` (the first broker round trip). Mirrors
``WorkerSettings.reload_factory_timeout``'s default — the SAME bound the
worker applies to every factory call it makes (worker/deps.py), not a
second mechanism. A hung dependency fails UI startup loudly instead of
parking the admin server forever."""

_UI_PG_PING_TIMEOUT_SECS: Final[float] = 0.2
"""Bounds the ``/jobs/health/ready`` PG probe (acquire + SELECT 1).
Mirrors ``WorkerSettings.health_pg_ping_timeout``'s default — the bound
the worker's readiness ping (worker/health.py) applies to the identical
probe; an unbounded probe turns a wedged PG into a wedged prober."""

_UI_POOL_COMMAND_TIMEOUT_SECS: Final[float] = 5.0
"""Per-query ``command_timeout`` for the UI's admin pool. Mirrors
``WorkerSettings.dispatcher_command_timeout``'s default — the per-query
bound on every other pool the repo builds. One pool-level bound covers
every admin-page query on the pool (the sweep's ~20 admin query sites);
without it a black-holed PG wedges each admin request forever."""

app = typer.Typer(
    name="taskq",
    no_args_is_help=True,
    help="TaskQ — async Postgres-backed background jobs.",
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
    help="Inspect individual jobs.",
)
app.add_typer(job_app, name="job")


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
    held. An empty registry is refused rather than returned — every
    downstream consumer checks ``is not None`` and cannot distinguish
    ``{}`` from a populated mapping, so a worker handed an empty
    registry boots and dispatches nothing. On any failure prints the
    reason to stderr and raises ``typer.Exit(code=1)`` — shared by
    ``worker`` and ``actor-config diff``.

    That sharing is a deliberate trade-off for the read-only ``diff``
    command: it too exits 1 on an empty registry, because the shared
    loader cannot tell "an operator auditing stored rows against an
    intentionally empty registry" from "a misconfigured ``--actors``
    ref that resolved to nothing" — and the second is far more likely.
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
        # element type is Any here — annotating ActorRef would make the
        # guard look dead to the type checker.
        items: list[Any] = list(raw)
        if not all(isinstance(r, ActorRef) for r in items):
            typer.echo(
                "expected Mapping[str, ActorRef] or Iterable[ActorRef] at "
                f"{actors}; got {type(raw).__name__}",
                err=True,
            )
            raise typer.Exit(code=1)
        registry = {r.name: r for r in items}
    else:
        typer.echo(
            "expected Mapping[str, ActorRef] or Iterable[ActorRef] at "
            f"{actors}; got {type(raw).__name__}",
            err=True,
        )
        raise typer.Exit(code=1)

    if not registry:
        typer.echo(
            f"actor registry at {actors} is empty — a worker with no actors "
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
    **class** itself when its constructor takes no required arguments —
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
            "returning one, or the provider class — see docs/guides/managed-identities.md.",
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

    The refs used to be read by Typer's ``envvar=``, which sees
    ``os.environ`` and nothing else. They are now ordinary settings, so
    they take part in dotenvmodel's ``.env`` cascade like everything else;
    the flag-wins precedence Typer gave them is preserved here.
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
    PG ref is set) — the worker-internal per-slot transaction pool does
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
        "assignment is unaffected — boots never rewrite it; move an actor with "
        "`taskq actor-config move-queue`. Capacity fields (max_concurrent / "
        "max_pending / result_ttl) are likewise unaffected — use `taskq "
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
        f"{_PROVIDER_EXAMPLE}) — an instance, a zero-arg factory returning one, "
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
    setup_logging(level=settings.log_level, log_format=settings.log_format)
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
        # itself (see exceptions.py) — don't print it a second time here.
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1) from None
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
        typer.echo(f"Error: cannot import '{module_name}' — module not found", err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(f"Error: cannot import '{module_name}' — {exc}", err=True)
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
    typer.echo(f"TaskQ dev mode — watching {watch_display}. Press Ctrl-C to stop.", err=True)

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
    finally:
        # Why bounded: a dead PG can block close() indefinitely, wedging even
        # this one-shot command before process exit. The
        # helper terminates on timeout and never raises, so a close error can
        # no longer mask an in-flight exception from list_applied.
        await close_conn_bounded(conn, "migrate-status", CLOSE_TIMEOUT_SECS)
    typer.echo(f"schema: {settings.schema_name}")
    typer.echo(f"applied: {len(applied)}")
    for migration in migrate_mod.discover():
        marker = "✔" if migration.key in applied else " "
        suffix = "" if migration.use_transaction else " (no transaction)"
        typer.echo(f"  [{marker}] {migration.filename}{suffix}")


async def _up(
    settings: TaskQSettings,
    *,
    phase: migrate_mod.Phase | None,
    target: str | None,
    max_steps: int | None,
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
    # get the report — and the close is skipped when no conn was acquired.
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
            )
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
            "(no worker restart) — the dispatch query re-reads this column every cycle.",
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
            "— no redeploy, no worker restart.",
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
            "change (no worker restart) — the terminal-write UPDATE re-reads this column "
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
            "nothing to change — pass at least one --max-concurrent/--max-pending/--result-ttl "
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
        # errors — print the reason, not a traceback.
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None
    finally:
        await close_conn_bounded(conn, "actor-config-set", CLOSE_TIMEOUT_SECS)
    if row is None:
        typer.echo(
            f"no stored actor_config row for actor {actor!r} — it must be registered by a "
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
    trail but are ROUTED at dispatch by the actor's current assignment —
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
        # A drain batch ran out of its deadline — server-side
        # (statement_timeout) or client-side (dropped connection, cancelled
        # await). Both are the same fact to an operator: this batch did not
        # land. Catching the named family rather than spelling the pair here
        # is what keeps the client half from escaping as an untyped error.
        # The batches that
        # committed before it are real progress and the drain's queue
        # predicate skips rows already moved, so the only action is to run
        # the command again — which is a refusal to report, not a crash.
        # The error text is not echoed: the server appends DETAIL quoting
        # row values, which must not cross this boundary.
        typer.echo(
            f"move-queue of actor {actor!r} onto {new_queue!r} aborted: a drain batch "
            "exceeded its statement timeout. Batches committed before the abort are "
            "kept, so the move is incomplete and safe to re-run — re-run the same "
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
            help="Target queue name. Required — the target is never defaulted.",
        ),
    ],
) -> None:
    """Move an actor onto a different queue, and report the residual.

    Applies the coordinated writes of a move — the stored assignment and
    the target `queues` row carrying the source's mode and cap — in one
    transaction, so a failure leaves the deployment exactly where it
    started rather than half-moved. The actor's pending/scheduled backlog
    is rewritten onto the target first, as bounded committed batches.

    The target is named by `--to` rather than positionally: the two
    arguments of a move are an actor and a queue, both plain strings, and
    two bare positionals are the shape an operator transposes under
    pressure — with the consequence that the backlog drains onto a queue
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
    until it drains), not part of the move's result record — the move
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
        # residual to drain — at zero the condition is already met and
        # repeating it reads as an outstanding action.
        residual_line += (
            " Keep that queue's consumers running until every producer carries "
            "the new literal and this count reaches zero."
        )
    typer.echo(residual_line, err=True)
    typer.echo(
        f"NOTE: ensure workers consume {result.to_queue!r} now, and keep "
        f"consuming {result.from_queue!r} until every producer runs the "
        f"matching literal — stale producers keep enqueueing to "
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
    is read by the dispatch SQL, which cannot see the code literal — once
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

    Three states fail: a queue mismatch (assignment drift — boot adopts the
    stored queue, but the cron leader's fires follow it while producers
    enqueue by their own literal, so the two routing halves disagree until
    the move or the deploy completes), a metadata mismatch (the next worker
    startup raises ActorConfigDriftList), and a registry actor with no
    stored row (the dispatch capacity gate reads only actor_config rows, so
    the actor does not dispatch until a row is seeded). Capacity-only
    differences never fail — stored capacity is operator-owned by design —
    and neither does a leftover row for an actor that is no longer
    registered: it only serves already-queued jobs.
    """
    typer.echo(f"{name}:")
    if row is None:
        # Registry-only actor: nothing has ever seeded a row. Enforcement
        # differs per field: the dispatch CTE builds its candidate gate
        # FROM actor_config (inner join), so with no row the actor is
        # NEVER dispatched — max_concurrent is effectively 0, not the
        # literal. max_pending / result_ttl enforcement can see the code
        # literal, so those fall back to it.
        assert ref is not None  # row is None only when the name came from the registry
        typer.echo(
            "  no stored row — never synced; the row is seeded at the next "
            "worker startup. Until then the actor DOES NOT DISPATCH (the "
            "dispatch capacity gate reads only actor_config rows)."
        )
        for field in _CAPACITY_DIFF_FIELDS:
            literal = _literal_for_field(ref, field)
            if field == "max_concurrent":
                typer.echo(
                    f"  {field:<15} literal={literal}  effective=0 (no stored row — "
                    "actor cannot dispatch)"
                )
            else:
                typer.echo(f"  {field:<15} literal={literal}  effective={literal} (literal)")
        typer.echo(f"  {'queue':<15} literal={ref.queue}")
        return True
    if ref is None:
        typer.echo(
            "  stored row's actor is not in the registry — leftover row; "
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
            f"  {'queue':<15} literal={ref.queue}  stored={row.queue}  MISMATCH — assignment "
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
            f"  {'metadata':<15} literal={dict(ref.metadata)}  stored={row.metadata}  MISMATCH — "
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
    runtime — the stored value wins; tune it with `taskq actor-config
    set` — and a queue mismatch means the two routing halves disagree
    (boot adopts the stored queue and cron fires follow it while producers
    enqueue by their own literal); reconcile with `taskq actor-config
    move-queue`. A metadata mismatch still refuses the next worker
    startup with ActorConfigDriftList.

    Exit codes: 0 no gate-failing drift; 1 at least one actor fails the
    gate — a registry actor with no stored row (it does not dispatch until
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
        concurrent = "uncapped (stored NULL — no actor-level cap)"
    elif row.max_concurrent == 0:
        concurrent = "0 — DRAIN MODE (deliberately stopped; jobs enqueue and never run)"
    else:
        concurrent = str(row.max_concurrent)
    pending = "unlimited (stored NULL)" if row.max_pending is None else str(row.max_pending)
    return f"max_concurrent={concurrent}  max_pending={pending}"


@dataclass(frozen=True, slots=True)
class _StrandedActorJobs:
    """One actor's stranded pending/scheduled rows, by strand shape.

    Mirrors the two shapes the leader's stranded-jobs sweep computes
    (``_stranded_jobs_loop`` in ``taskq/worker/_leader_sweeps.py``):
    ``no_actor_config`` rows can never become dispatch candidates, and
    ``unserved_queue`` rows route to a queue no live worker serves.
    """

    actor: str
    no_actor_config: int
    unserved_queue: int
    unserved_queues: tuple[str, ...]


async def _list_stranded_pending_jobs(
    conn: asyncpg.Connection, *, schema: str
) -> list[_StrandedActorJobs]:
    """Pending/scheduled jobs grouped by the actor nothing alive consumes.

    The same computation the leader's stranded-jobs sweep runs every
    minute, issued here on demand: ``doctor`` is the surface an operator
    reaches for mid-incident, and it cannot wait on a leader tick.  The
    routing-queue discriminator (a re-pended row routes by its actor's
    stored assignment, not its label) is dispatch's own contract, mirrored
    from the sweep so both surfaces answer the same question the same way.
    The result is per ACTOR — bounded by the distinct-actor count, never
    by backlog depth.
    """
    if not _IDENT_RE.match(schema):
        # Defence in depth: TaskQSettings validates schema_name at load;
        # re-check at the SQL interpolation site (the queue_ops convention).
        raise ValueError(f"invalid schema identifier: {schema!r}")
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
GROUP BY s.actor"""  # noqa: S608  # Why: schema is identifier-validated above and double-quoted; no user values are interpolated.
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


def _doctor_findings(
    registry: Mapping[str, ActorRef[Any, Any]],
    rows: list[ActorConfigRow],
    queues: list[QueueRow],
    stranded: list[_StrandedActorJobs],
) -> list[str]:
    """Every condition worth an operator's attention, as report lines.

    Every family is a condition TaskQ has decided a worker keeps
    running through, which is why they surface here rather than at boot:
    each one produces no error anywhere, and its only symptom is work
    that quietly does not happen.
    """
    stored_by_actor = {row.actor: row for row in rows}
    findings: list[str] = []

    # The dispatch capacity gate joins actor_config, so a registered actor
    # with no row is not merely uncapped — it is never a candidate.
    for name in sorted(set(registry) - set(stored_by_actor)):
        findings.append(
            f"{name}: no stored actor_config row — NEVER DISPATCHES. The dispatch "
            "capacity gate reads only stored rows, so jobs accumulate pending "
            "with no error anywhere. A worker startup seeds the row."
        )

    # The same gate seen from the jobs side: rows already pending/scheduled
    # whose actor has no stored config row (a renamed or removed actor that
    # old producers or old rows still reference) never dispatch either, and
    # no registry walk can name them — the registry no longer knows the name.
    # The unserved-queue arm is the fleet-liveness twin: the row's routing
    # queue (its actor's stored assignment once re-pended) has no live
    # worker subscribed, so every dispatch round annihilates the pair.
    for entry in sorted(stranded, key=lambda e: e.actor):
        if entry.no_actor_config:
            registry_note = (
                " and no entry in the loaded registry" if entry.actor not in registry else ""
            )
            findings.append(
                f"{entry.actor}: {entry.no_actor_config} pending/scheduled job(s) whose "
                f"actor has no stored actor_config row{registry_note} — NEVER DISPATCHES. "
                "The dispatch capacity gate reads only stored rows, so these jobs wait "
                "forever with no error anywhere. Re-register the actor and seed its row "
                "(a worker startup does this), or purge the jobs if the actor was retired."
            )
        if entry.unserved_queue:
            queue_names = ", ".join(repr(q) for q in entry.unserved_queues)
            findings.append(
                f"{entry.actor}: {entry.unserved_queue} pending/scheduled job(s) routed to "
                f"queue(s) {queue_names} that no live worker serves — they wait while "
                "nothing consumes them. Start a worker subscribed to the queue or move "
                "the actor onto a served one."
            )

    # A queues row whose queue no actor is assigned to is inert until an
    # actor is moved onto that name and silently inherits its cap.
    assigned = {row.queue for row in stored_by_actor.values()}
    for queue in sorted(
        (q for q in queues if q.name not in assigned and q.max_concurrent is not None),
        key=lambda q: q.name,
    ):
        findings.append(
            f"queue {queue.name!r}: STALE queues row — max_concurrent="
            f"{queue.max_concurrent} but no actor is assigned to it. The cap is "
            "inert now and silently applies to the next actor moved onto this queue."
        )

    queue_caps = {q.name: q.max_concurrent for q in queues}
    for name in sorted(stored_by_actor):
        row = stored_by_actor[name]
        # Neither value is invalid alone: only the combination is
        # unsatisfiable, so only a combination check can catch it.
        if (
            row.max_concurrent is not None
            and row.max_pending is not None
            and row.max_pending < row.max_concurrent
        ):
            findings.append(
                f"{name}: INCOHERENT — max_pending={row.max_pending} is below "
                f"max_concurrent={row.max_concurrent}, so the actor may queue fewer "
                "jobs than it may run at once and its concurrency cap is unreachable."
            )
        queue_cap = queue_caps.get(row.queue)
        if (
            row.max_concurrent is not None
            and queue_cap is not None
            and queue_cap < row.max_concurrent
        ):
            findings.append(
                f"{name}: INCOHERENT — max_concurrent={row.max_concurrent} exceeds "
                f"queue {row.queue!r}'s max_concurrent={queue_cap}, which binds first; "
                "raising the actor cap alone changes nothing."
            )
    return findings


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
) -> None:
    """Report capacity and configuration conditions that fail silently.

    TaskQ refuses boot only on structural stored-config drift, so a whole
    family of misconfigurations produces no error at all: an actor with no
    stored row never dispatches, a leftover `queues` row caps an actor
    nobody thinks is capped, a stored `max_concurrent=0` drains an
    actor that looks configured, and a job already pending for an actor
    nothing consumes waits forever. Each one's only symptom is work that
    does not happen. This is the one command that names them together.

    Read-only: it issues no writing statement, so it is safe to run
    against production mid-incident.

    Exit code: always 0. Every condition reported here is one a worker
    keeps running through, and a diagnostic that fails the shell gets
    wrapped in `|| true` and then ignored. Gating CI on drift is
    `taskq actor-config diff`, which exits non-zero by design.
    """
    registry = _load_actor_registry(actors)
    settings = TaskQSettings.load()
    asyncio.run(_doctor(settings, registry))


async def _doctor(
    settings: TaskQSettings,
    registry: Mapping[str, ActorRef[Any, Any]],
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        rows = await list_actor_configs(conn, schema=settings.schema_name)
        queues = await list_queues(conn, schema=settings.schema_name)
        stranded = await _list_stranded_pending_jobs(conn, schema=settings.schema_name)
    finally:
        await close_conn_bounded(conn, "doctor", CLOSE_TIMEOUT_SECS)

    typer.echo("stored actor capacity:")
    for row in sorted(rows, key=lambda r: r.actor):
        typer.echo(f"  {row.actor:<20} queue={row.queue:<16} {_describe_actor_capacity(row)}")
    if not rows:
        typer.echo("  (no stored actor_config rows)")

    findings = _doctor_findings(registry, rows, queues, stranded)
    typer.echo("")
    if not findings:
        typer.echo("no findings — every registered actor has a stored row and every")
        typer.echo("queue row backs a live assignment.")
        return
    typer.echo(f"findings ({len(findings)}):")
    for finding in findings:
        typer.echo(f"  - {finding}")


async def _report_up_failure(conn: asyncpg.Connection | None, schema: str, exc: Exception) -> None:
    """Print a self-diagnosing ``migrate up`` failure report to stderr.

    TaskQ users must never inspect catalog state by hand, so this reports —
    gathered on the still-open connection — what failed, what state the
    schema is in (INVALID indexes included), and the single action to take.
    The diagnosis lives in :mod:`taskq.migrate` (shared with the
    worker/startup path).

    The report must NEVER mask the original error: when the conn was never
    acquired (connect itself failed) or the diagnosis itself raises, the
    fallback is the generic two-line report — the original error's headline
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
            # every SAML deployment to 8h and made TASKQ_SAML_SESSION_MAX_AGE_SECONDS — a
            # documented, `ge=60`-validated knob — a no-op. It bounds the itsdangerous
            # signature max_age (auth/_session.py), i.e. how long a stolen admin cookie stays
            # valid, so an operator shortening it must actually take effect.
            session_max_age_seconds=saml.session_max_age_seconds,
            secure_cookie=secure,
            group_attribute=saml.group_attribute,
            allowed_groups=saml.allowed_groups_set,
        )
        return create_saml_auth(config, base_path=base_path)
    return None


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
) -> None:
    from contextlib import asynccontextmanager

    from fastapi import APIRouter, Depends, FastAPI, Response
    from fastapi.responses import RedirectResponse

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
            # nobody sequences — a pod restart, a rollout, an autoscale
            # event — not on an operator's decision. Post-phase migrations
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
            # A credential-provider pool passes password= as an async
            # callable, so every physical connection this long-lived UI
            # process opens re-authenticates with a fresh token; the DSN
            # path is unchanged.
            if pool_factory is not None:
                # Why bounded: UI startup arms no watchdog — a hung token
                # endpoint inside the factory would park `taskq ui serve`
                # forever before any request is served.
                # _UI_FACTORY_TIMEOUT_SECS is the SAME bound the worker
                # applies to its bootstrap factory calls (worker/deps.py).
                try:
                    pg_pool = await asyncio.wait_for(
                        pool_factory(), timeout=_UI_FACTORY_TIMEOUT_SECS
                    )
                except TimeoutError as exc:
                    raise TimeoutError(
                        f"taskq ui serve: pool_factory did not return within "
                        f"{_UI_FACTORY_TIMEOUT_SECS}s — the credential "
                        "provider behind it (e.g. a token endpoint) is "
                        "black-holed. UI startup fails loudly instead of "
                        "parking forever."
                    ) from exc
            else:
                # settings (the TaskQSettings this UI was launched with) is
                # in scope, so the pair resolves through statement_cache_kwargs;
                # forwarded explicitly so pyright can trace types through
                # asyncpg.create_pool.
                stmt_kwargs = statement_cache_kwargs(settings)
                # Why command_timeout: every admin-page query runs on this
                # pool — the pool-level per-query bound that closes all of
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
                )
            assert pg_pool is not None, "asyncpg.create_pool returned None"
            pool = pg_pool

            async def _close_ui_pool() -> None:
                # Why module-global reads at call time: tests monkeypatch
                # close_pool_bounded / CLOSE_TIMEOUT_SECS as
                # observation and timeout-shrink seams (same convention as
                # taskq.worker.deps).
                await close_pool_bounded(pool, "ui-admin", CLOSE_TIMEOUT_SECS)

            # Why a pushed callback instead of stack.enter_async_context(pool):
            # Pool.__aexit__ closes UNBOUNDED — a dead PG would wedge UI
            # shutdown. The bounded helper terminates the pool on
            # timeout and never raises.
            stack.push_async_callback(_close_ui_pool)

            redis_client: object | None = None
            if redis_url is not None:
                try:
                    import redis.asyncio as aioredis
                except ImportError as exc:
                    raise ImportError(
                        "redis_url is configured but the [redis] extra is not installed. "
                        "Install it with: pip install 'taskq[redis]'"
                    ) from exc

                if redis_factory is not None:
                    # Why bounded: same first-use factory discipline as the
                    # pool factory above — the worker's reload bounds its
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
                            f"within {_UI_FACTORY_TIMEOUT_SECS}s — the Redis "
                            "credential provider is black-holed. UI startup "
                            "fails loudly instead of parking forever."
                        ) from exc
                else:
                    client = aioredis.from_url(redis_url)

                # Why not stack.enter_async_context(client): Redis.__aexit__
                # calls aclose() UNBOUNDED (and shielded) — a hung broker
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
                # — the unwind runs the pushed callback through the bounded
                # close (never raises; aclose() on a never-initialized client
                # is a no-op).
                stack.push_async_callback(_close_ui_redis)
                # Why bounded: initialize() is the eager first broker round
                # trip — a black-holed Redis would park UI startup forever,
                # and the UI process arms no watchdog. Same wait_for
                # discipline as JobsClient._open_redis; the pushed callback
                # above already bounds the unwind's close.
                try:
                    await asyncio.wait_for(client.initialize(), timeout=_UI_FACTORY_TIMEOUT_SECS)
                except TimeoutError as exc:
                    raise TimeoutError(
                        f"taskq ui serve: Redis initialize() did not complete "
                        f"within {_UI_FACTORY_TIMEOUT_SECS}s — the broker at "
                        f"{redis_url} is unreachable or black-holed. UI "
                        "startup fails loudly instead of parking forever."
                    ) from exc
                redis_client = client

            bundle = create_router(
                pool,
                schema=schema,
                redis_client=redis_client,
                auth_dependency=auth_dependency,
                base_path="/admin",
            )

            setup_admin_state(application, bundle)
            application.include_router(bundle.router, prefix="/admin")
            if sso_bundle is not None:
                application.include_router(sso_bundle.router, prefix="/admin")

            health_router = APIRouter(
                prefix="/jobs/health",
                tags=["health"],
                dependencies=health_deps,
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
                    # PG into a wedged prober.
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
    if resolved_pg_provider_ref is not None:
        pg_provider = _load_pg_credential_provider(
            resolved_pg_provider_ref, option="--pg-credential-provider"
        )
        # Why command_timeout: this factory builds the UI's admin pool —
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
        )
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
        redis_factory = make_redis_client_factory(
            resolved_redis,
            _load_redis_credential_provider(
                resolved_redis_provider_ref, option="--redis-credential-provider"
            ),
        )

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
    )


def _ensure_cwd_on_sys_path() -> None:
    """Put the current working directory on ``sys.path`` if it is absent.

    A console script starts with a ``sys.path`` that excludes the cwd, so
    ``taskq worker --actors myapp.actors:registry`` could not resolve the
    application's modules when run from its own project directory.
    ``python -m taskq`` prepends the cwd itself; inserting it here gives
    the console script the same import semantics (the ``python -m celery
    -A myapp worker`` shape) before any ``module:attr`` resolution runs.
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

    typer.echo(f"config OK — {len(cfg.workers)} worker(s), actors={cfg.actors!r}")
    for w in cfg.workers:
        health = "health=on" if w.health.enabled else "health=off"
        typer.echo(
            f"  {w.name}: queues={w.queues} "
            f"poll={w.poll_interval}s concurrency={w.max_concurrency} {health}"
        )


if __name__ == "__main__":
    main()


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
"""The operator-facing columns ``taskq job show`` prints.

Explicit, not ``SELECT *``: the printed set is the contract, and leaving
``payload``/``result``/``progress_state``/``error_traceback`` out keeps a
terminal-friendly read from dragging arbitrarily large blobs onto the
wire. Both ``jobs`` and ``jobs_archive`` carry every column listed.
"""


def _format_max_attempts(max_attempts: int, retry_kind: str) -> str:
    """The ``max_attempts`` display for a job row.

    Under ``retry_kind='indefinite'`` the stored ceiling is inert — the
    retry path never consults it — so printing the number would claim a
    budget the job does not carry. Render the inertness instead, the same
    framing the retries guide gives the field on an indefinite job.
    """
    if parse_retry_kind(retry_kind) == "indefinite":
        return "— (indefinite)"
    return str(max_attempts)


@job_app.command("show")
def job_show(
    job_id: Annotated[str, typer.Argument(help="Job id (UUID).")],
) -> None:
    """Show one job's stored row, from `jobs` or `jobs_archive`."""
    settings = TaskQSettings.load()
    asyncio.run(_job_show(settings, job_id))


async def _job_show(settings: TaskQSettings, job_id: str) -> None:
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
        typer.echo(f"error_class: {row['error_class']}")
        typer.echo(f"error_message: {row['error_message']}")
    if row["idempotency_key"] is not None:
        typer.echo(f"idempotency_key: {row['idempotency_key']}")
    if archived:
        typer.echo("archived: yes")
