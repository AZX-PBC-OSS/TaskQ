"""Audit-coverage pins: the remaining >5-line uncovered blocks.

Every test here was written red-first against a specific uncovered block
(the audit-cov census, branch feat/audit-coverage); each documents the
block it closes, and each was mutation-proven (flipping a target line
makes the test fail).

Closed blocks:
  - ``client._jobs._nul_rejected_field``: the metadata and tags arms of
    the NUL-attribution re-serialization (the streaming boundary's
    per-item locator, _jobs.py:202-210).
  - ``_di.scopes``: the CLASS factory arm's generator-lifecycle
    invariant tripwire (scopes.py:255-260) — reachable through a directly
    constructed ProviderEntry, which is how a registration bug surfaces.
  - ``cli``: the ``migrate disable-hypertables`` success report
    (cli.py:881-892).
  - ``testing._shared_containers.services_have_live_owner``: the
    label-vet reuse decision (unlabeled containers kept, docker hiccups
    fail open, live owners keep) (567-579).
"""

import asyncio
import os
import subprocess  # Why: the test mints a dead pid, it does not shell out for logic
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

import taskq.cli as cli_mod
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
from taskq._di.types import FactoryShape, ProviderEntry, ProviderLifecycle
from taskq.client._jobs import (  # pyright: ignore[reportPrivateUsage]  # Why: the stream-boundary locator under test
    _nul_rejected_field,
)
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.assertions import plain_cli_output
from taskq.testing.jobs import make_enqueue_args
from tests._di_scopes import make_scopes

runner = CliRunner()


# ── the NUL locator's metadata and tags arms ────────────────────────────


def test_nul_rejected_field_locates_a_nul_metadata_value() -> None:
    """A NUL that reached the metadata dict AFTER construction (the
    chokepoint only guards construction) is located as 'metadata', the
    stream-boundary annotation's field."""
    args = make_enqueue_args(payload={})
    assert args.metadata is not None
    args.metadata["tenant"] = "ac\x00me"
    assert _nul_rejected_field(args, idx=0) == "metadata"


def test_nul_rejected_field_locates_a_nul_tag() -> None:
    """A NUL that reached the tags tuple (only possible by bypassing the
    construction chokepoint — the guard keeps the serialization layer
    honest about what it actually binds) is located as 'tags'."""
    args = make_enqueue_args(payload={})
    object.__setattr__(args, "tags", ("clean", "a\x00b"))
    assert _nul_rejected_field(args, idx=0) == "tags"


def test_nul_rejected_field_answers_none_for_a_clean_item() -> None:
    """A clean item locates nothing — the no-annotation fast path."""
    args = make_enqueue_args(payload={"v": 1}, tags=("clean",))
    assert _nul_rejected_field(args, idx=0) is None


# ── the CLASS factory arm's lifecycle invariant ─────────────────────────


class _Service:
    def __init__(self) -> None:
        self.started = True


def _scopes() -> tuple[ProcessScope, ThreadScope, LoopScope]:
    registry = ProviderRegistry()
    process, thread, loop = make_scopes(registry)
    return process, thread, loop


@pytest.mark.parametrize(
    "lifecycle",
    [
        ProviderLifecycle.AsyncGenerator,
        ProviderLifecycle.SyncGenerator,
        ProviderLifecycle.PlainFactory,
    ],
)
def test_class_arm_refuses_generator_lifecycles(lifecycle: ProviderLifecycle) -> None:
    """A CLASS factory_shape entry whose lifecycle says generator is a
    registration bug (register_class's own detection can never produce
    one); the arm raises the invariant tripwire rather than mis-closing
    the instance — pinned through the direct-entry construction a registry
    bug would take."""
    asyncio.run(_class_arm_refuses(lifecycle))


async def _class_arm_refuses(lifecycle: ProviderLifecycle) -> None:
    registry = ProviderRegistry()
    entry = ProviderEntry(
        type_=_Service,
        scope=Scope.PROCESS,
        kind="class",
        impl=_Service,
        factory_shape=FactoryShape.CLASS,
        lifecycle=lifecycle,
    )
    registry._providers[_Service] = entry  # pyright: ignore[reportPrivateUsage]  # Why: the invariant tripwire is only reachable through a hand-built entry
    process, thread, loop = _scopes()
    try:
        with pytest.raises(RuntimeError, match="reached CLASS arm"):
            await loop.get_or_create(_Service, entry)
    finally:
        await loop.shutdown()
        await thread.shutdown()
        await process.shutdown()


# ── the disable-hypertables success report ──────────────────────────────


class _OffSettings:
    """TaskQSettings double: hypertables off, no real env cascade."""

    @classmethod
    def load(cls) -> TaskQSettings:
        return TaskQSettings.load_from_dict(
            {"TASKQ_TIMESCALEDB_HYPERTABLES": "false", "TASKQ_PG_DSN": "postgresql://u:p@h/d"}
        )


class _OffWorkerSettings:
    """WorkerSettings double: same cascade, zero-statement gate off."""

    @classmethod
    def load(cls) -> WorkerSettings:
        return WorkerSettings.load_from_dict(
            {"TASKQ_TIMESCALEDB_HYPERTABLES": "false", "TASKQ_PG_DSN": "postgresql://u:p@h/d"}
        )


def _patch_disable_cli(monkeypatch: pytest.MonkeyPatch, report: Any) -> AsyncMock:
    async def _no_lock(conn: Any, *args: Any, **kwargs: Any) -> AsyncIterator[None]:
        del conn, args, kwargs
        yield

    monkeypatch.setattr(cli_mod, "TaskQSettings", _OffSettings)
    monkeypatch.setattr(cli_mod, "WorkerSettings", _OffWorkerSettings)
    monkeypatch.setattr(cli_mod, "_open_migrate_conn", AsyncMock(return_value=MagicMock()))
    migrate_mod = __import__("taskq.migrate", fromlist=["migration_advisory_lock"])
    monkeypatch.setattr(
        migrate_mod,
        "migration_advisory_lock",
        asynccontextmanager(_no_lock),  # pyright: ignore[reportDeprecated]  # Why: the stdlib CM factory is the double's shape
    )
    disable_mock = AsyncMock(return_value=report)
    monkeypatch.setattr(cli_mod, "disable_hypertables", disable_mock)
    return disable_mock


def test_disable_hypertables_prints_the_success_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The disable command's success arm: the report's converted tables,
    removed retention policies and removed compression policies each print
    a line."""
    from taskq.timescale import HypertableReport

    report = HypertableReport(
        converted=("job_events",),
        retention_policies=("job_events:30 days",),
        compression_policies=("attempts:7 days",),
    )
    _patch_disable_cli(monkeypatch, report)
    result = runner.invoke(
        cli_mod.app,
        ["migrate", "disable-hypertables"],
        env={"TASKQ_TIMESCALEDB_HYPERTABLES": "false", "TASKQ_PG_DSN": "postgresql://u:p@h/d"},
    )
    assert result.exit_code == 0, result.output
    assert "disabled hypertables on 1 table(s):" in plain_cli_output(result.output)
    assert "restored to plain: job_events" in plain_cli_output(result.output)
    assert "removed retention policy: job_events:30 days" in plain_cli_output(result.output)
    assert "removed compression policy: attempts:7 days" in plain_cli_output(result.output)


def test_disable_hypertables_no_work_re_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """A converged re-run (nothing left to disable) says so and exits 0."""
    from taskq.timescale import HypertableReport

    _patch_disable_cli(monkeypatch, HypertableReport())
    result = runner.invoke(
        cli_mod.app,
        ["migrate", "disable-hypertables"],
        env={"TASKQ_TIMESCALEDB_HYPERTABLES": "false", "TASKQ_PG_DSN": "postgresql://u:p@h/d"},
    )
    assert result.exit_code == 0, result.output
    assert "no hypertables to disable" in plain_cli_output(result.output)


# ── the shared-container reuse decision ─────────────────────────────────


def _info(pg_id: str, redis_id: str) -> Any:
    from taskq.testing._shared_containers import SharedServices

    return SharedServices(
        pg_dsn="postgresql://u:p@h:5432/d",
        pg_container_id=pg_id,
        redis_host="h",
        redis_port=6379,
        redis_container_id=redis_id,
    )


def _docker_double(labels: dict[str, str], *, raises: bool = False) -> Any:
    container = MagicMock()
    container.labels = labels
    client = MagicMock()
    if raises:
        from docker.errors import DockerException

        client.containers.get = MagicMock(side_effect=DockerException("daemon down"))
    else:
        client.containers.get = MagicMock(return_value=container)
    return client


def test_services_keep_unlabeled_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Containers predating the owner-label scheme have no owner to
    contradict the reuse — kept (there is no owner information)."""
    import taskq.testing._shared_containers as sc

    monkeypatch.setattr(sc, "_docker_client", lambda: _docker_double({}))
    assert sc.services_have_live_owner(
        _info("pg", "redis")  # type: ignore[arg-type]
    )


def test_services_fail_open_on_a_docker_hiccup(monkeypatch: pytest.MonkeyPatch) -> None:
    """An inspection error keeps the containers: a docker hiccup must
    never break suite startup, ``container_running`` already vetted them."""
    import taskq.testing._shared_containers as sc

    monkeypatch.setattr(sc, "_docker_client", lambda: _docker_double({}, raises=True))
    assert sc.services_have_live_owner(
        _info("pg", "redis")  # type: ignore[arg-type]
    )


def test_services_live_owner_keeps_and_dead_owner_ignores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A labeled container's live owner pid keeps the pair; a dead owner's
    pid releases it for reclaim."""
    import taskq.testing._shared_containers as sc

    label_key = "any-repo.test.creator-pid"
    assert sc.OWNER_PID_LABEL_RE.match(label_key)

    live = _docker_double({label_key: str(os.getpid())})
    monkeypatch.setattr(sc, "_docker_client", lambda: live)
    assert sc.services_have_live_owner(
        _info("pg", "redis")  # type: ignore[arg-type]
    )

    proc = subprocess.Popen(["true"])  # noqa: S607  # Why: mints a reaped (dead) pid
    proc.wait()
    dead = _docker_double({label_key: str(proc.pid)})
    monkeypatch.setattr(sc, "_docker_client", lambda: dead)
    assert not sc.services_have_live_owner(
        _info("pg", "redis")  # type: ignore[arg-type]
    )
