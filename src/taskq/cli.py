"""``taskq`` CLI entry point.

The CLI is intentionally thin today — only the commands needed to bootstrap
a database. Worker and client commands will be added as those subsystems
land.

Usage::

    taskq migrate status
    taskq migrate up [--phase pre|post] [--target VERSION] [--max-steps N]
"""

import asyncio
import contextlib
import importlib
from collections.abc import AsyncGenerator, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Final, cast

import asyncpg
import structlog
import typer

from taskq import migrate as migrate_mod
from taskq._close import (
    CLOSE_TIMEOUT_SECS,
    close_conn_bounded,
    close_pool_bounded,
    close_redis_bounded,
)
from taskq.actor import ActorRef
from taskq.exceptions import ActorConfigDriftList
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.worker.actor_config_ops import (
    UNSET,
    ActorConfigRow,
    Unset,
    get_actor_config,
    list_actor_configs,
    set_actor_config_capacity,
)
from taskq.worker.dev import dev_watch_loop
from taskq.worker.run import worker_main as _worker_main

logger: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.cli")

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


def _load_actor_registry(actors: str) -> Mapping[str, ActorRef[Any, Any]]:
    """Resolve a ``module:attr`` reference to an actor registry.

    Accepts either ``Mapping[str, ActorRef]`` or an iterable of
    ``ActorRef`` (keyed by name). On any failure prints the reason to
    stderr and raises ``typer.Exit(code=1)`` — shared by ``worker`` and
    ``actor-config diff``.
    """
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
        typer.echo(f"module not found: {module_name}", err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(f"failed to import module {module_name}: {exc}", err=True)
        raise typer.Exit(code=1) from None

    try:
        raw = getattr(module, attr_name)
    except AttributeError:
        typer.echo(
            f"attribute {attr_name!r} not found in module {module_name}",
            err=True,
        )
        raise typer.Exit(code=1) from None

    if isinstance(raw, Mapping):
        return cast(Mapping[str, ActorRef[Any, Any]], raw)
    if (
        not isinstance(raw, (str, bytes))
        and hasattr(raw, "__iter__")
        and all(isinstance(r, ActorRef) for r in raw)  # type: ignore[arg-type]  # Why: raw is object; pyright cannot verify iterability for the isinstance call.
    ):
        return {r.name: r for r in raw}  # type: ignore[union-attr]  # Why: the isinstance check ensures raw is Iterable[ActorRef]; pyright cannot narrow across the all() predicate inside elif.
    typer.echo(
        "expected Mapping[str, ActorRef] or Iterable[ActorRef] at "
        f"{actors}; got {type(raw).__name__}",
        err=True,
    )
    raise typer.Exit(code=1)


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
        help="Allow sync_actor_config to overwrite a stored actor_config row whose queue "
        "or metadata differ from the registered values. Use for one deploy to "
        "deliberately re-route an actor, then unset. Capacity fields "
        "(max_concurrent / max_pending / result_ttl) are unaffected — use "
        "`taskq actor-config set` for those. Equivalent to env var "
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
        "Exit 0 if all jobs succeeded, 2 if any failed, 3 if max-runtime "
        "was exceeded. Incompatible with cron-driven workloads.",
    ),
    idle_settle_window: float | None = typer.Option(
        None,
        "--idle-settle-window",
        help="Seconds to wait after queues appear empty before declaring "
        "drained. Overrides TASKQ_IDLE_SETTLE_WINDOW. Default 2.0.",
    ),
    idle_poll_interval: float | None = typer.Option(
        None,
        "--idle-poll-interval",
        help="How often to check queue depth. Overrides "
        "TASKQ_IDLE_POLL_INTERVAL. Default 1.0.",
    ),
    max_runtime: float | None = typer.Option(
        None,
        "--max-runtime",
        help="Maximum wall-clock seconds before forcing exit (code 3). "
        "Overrides TASKQ_IDLE_MAX_RUNTIME. Only used with --until-idle.",
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

    try:
        code = _worker_main(
            settings,
            actor_registry=registry,
            until_idle=until_idle,
            idle_settle_window=idle_settle_window,
            idle_poll_interval=idle_poll_interval,
            max_runtime=max_runtime,
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
def migrate_status() -> None:
    """Show applied and pending migrations."""
    settings = TaskQSettings.load()
    asyncio.run(_status(settings))


@migrate_app.command("up")
def migrate_up(
    phase: migrate_mod.Phase | None = typer.Option(
        None, "--phase", help="Restrict to 'pre' or 'post'."
    ),
    target: str | None = typer.Option(
        None, "--target", help="Stop after this version (inclusive). E.g. 01.00.00_01"
    ),
    max_steps: int | None = typer.Option(None, "--max-steps", help="Cap number of applies."),
) -> None:
    """Apply pending migrations."""
    settings = TaskQSettings.load()
    asyncio.run(_up(settings, phase=phase, target=target, max_steps=max_steps))


async def _status(settings: TaskQSettings) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        applied = await migrate_mod.list_applied(conn, settings.schema_name)
    finally:
        # Why bounded: a dead PG can block close() indefinitely, wedging even
        # this one-shot command before process exit (#38 follow-up). The
        # helper terminates on timeout and never raises, so a close error can
        # no longer mask an in-flight exception from list_applied.
        await close_conn_bounded(conn, "migrate-status", CLOSE_TIMEOUT_SECS)
    typer.echo(f"schema: {settings.schema_name}")
    typer.echo(f"applied: {len(applied)}")
    for migration in migrate_mod.discover():
        marker = "✔" if migration.key in applied else " "
        typer.echo(f"  [{marker}] {migration.filename}")


async def _up(
    settings: TaskQSettings,
    *,
    phase: migrate_mod.Phase | None,
    target: str | None,
    max_steps: int | None,
) -> None:
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        applied = await migrate_mod.apply_pending(
            conn,
            schema=settings.schema_name,
            phase=phase,
            target=target,
            max_steps=max_steps,
        )
    finally:
        # Why bounded: same dead-PG wedge risk as _status above (#38
        # follow-up); terminate-on-timeout, never raises.
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
        await conn.close()
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
        await conn.close()
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

    mc: int | None | Unset = UNSET
    if clear_max_concurrent:
        mc = None
    elif max_concurrent is not None:
        mc = max_concurrent

    mp: int | None | Unset = UNSET
    if clear_max_pending:
        mp = None
    elif max_pending is not None:
        mp = max_pending

    rt: float | None | Unset = UNSET
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
    max_concurrent: int | None | Unset,
    max_pending: int | None | Unset,
    result_ttl: float | None | Unset,
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
        await conn.close()
    if row is None:
        typer.echo(
            f"no stored actor_config row for actor {actor!r} — it must be registered by a "
            "worker startup first",
            err=True,
        )
        raise typer.Exit(code=1)
    _print_actor_config_row(row)


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
) -> None:
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
        return
    if ref is None:
        typer.echo(
            "  stored row's actor is not in the registry — leftover row; "
            "only already-queued jobs can still reference it"
        )
        _print_actor_config_row(row)
        return
    for field in _CAPACITY_DIFF_FIELDS:
        literal = _literal_for_field(ref, field)
        stored = getattr(row, field)
        effective, source = _effective_capacity(field, literal, row)
        typer.echo(
            f"  {field:<15} literal={literal}  stored={stored}  effective={effective} ({source})"
        )
    if ref.queue != row.queue:
        typer.echo(
            f"  {'queue':<15} literal={ref.queue}  stored={row.queue}  MISMATCH — structural "
            "drift; the next worker startup raises ActorConfigDriftList unless run with "
            "--force-update-actor-config"
        )
    else:
        typer.echo(f"  {'queue':<15} {row.queue} (match)")
    if dict(ref.metadata) != row.metadata:
        typer.echo(
            f"  {'metadata':<15} literal={dict(ref.metadata)}  stored={row.metadata}  MISMATCH — "
            "structural drift; the next worker startup raises ActorConfigDriftList unless run "
            "with --force-update-actor-config"
        )
    else:
        typer.echo(f"  {'metadata':<15} (match)")


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
    set` — while a queue/metadata mismatch blocks the next worker
    startup with ActorConfigDriftList.
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
        await conn.close()
    stored_by_actor = {row.actor: row for row in rows}

    names = sorted(set(registry) | set(stored_by_actor))
    if not names:
        typer.echo("no actors in the registry and no stored actor_config rows")
        return
    for name in names:
        _print_actor_diff(name, registry.get(name), stored_by_actor.get(name))


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
    secure = settings.environment not in {"dev", "development"}
    if backend == "oidc":
        from taskq.web.admin.auth import OIDCAuthConfig, create_oidc_auth

        oidc = settings.oidc
        config = OIDCAuthConfig(
            issuer=oidc.issuer,
            client_id=oidc.client_id,
            client_secret=oidc.client_secret,
            redirect_uri=oidc.redirect_uri,
            session_secret=oidc.session_secret,
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
            sp_private_key=saml.sp_private_key,
            session_secret=saml.session_secret,
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
) -> None:
    from contextlib import asynccontextmanager

    from fastapi import APIRouter, Depends, FastAPI, Response
    from fastapi.responses import RedirectResponse

    from taskq.web.admin import create_router, setup_admin_state

    sso_bundle = _build_sso_bundle(settings, base_path="/admin")
    auth_dependency = sso_bundle.dependency if sso_bundle is not None else None

    health_deps: list[Any] = []
    if settings.health_token:
        from taskq.web.admin.auth import token_auth

        health_deps = [Depends(token_auth(settings.health_token))]
    elif settings.environment not in {"dev", "development"}:
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
        from taskq.worker.health import (
            _check_live,  # pyright: ignore[reportPrivateUsage]  # Why: _check_live is a shared utility consumed by both transports (Unix socket + FastAPI); the underscore signals "internal to the health subsystem" not "private to health.py".
        )

        if run_migrate:
            await migrate_mod.apply_pending_locked(pg_dsn, schema=schema)

        async with AsyncExitStack() as stack:
            pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
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
            # shutdown (#38). The bounded helper terminates the pool on
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

                client = aioredis.from_url(redis_url)

                # Why not stack.enter_async_context(client): Redis.__aexit__
                # calls aclose() UNBOUNDED (and shielded) — a hung broker
                # would wedge UI shutdown (#38 follow-up). initialize()
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
                await client.initialize()
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
                    async with pool.acquire() as conn:
                        await conn.execute("SELECT 1")
                    ok = True
                    reasons: list[str] = []
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

    _ui_serve(
        resolved_dsn,
        resolved_schema,
        resolved_redis,
        resolved_host,
        resolved_port,
        resolved_migrate,
        settings,
    )


def main() -> None:
    """Console-script entry point."""
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
