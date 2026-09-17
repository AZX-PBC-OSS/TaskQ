"""Pins for workgroup → child credential-provider propagation.

The contract these tests pin, layer by layer:

* ``WorkerSpec.pg_credential_provider`` (directly or via the TOML loader)
  is forwarded to that worker's child command line as
  ``--pg-credential-provider module:attr`` — the flag the child CLI
  resolves. Environment inheritance (``TASKQ_PG_CREDENTIAL_PROVIDER``)
  covers a single fleet-wide provider only; the per-worker field is what
  lets a workgroup mix provider-backed and provider-less workers, which
  is what unblocks the Entra/managed-identity segment from adopting
  workgroups.
* A load-time validator refuses a provider ref the child CLI could never
  resolve (the child would die before registering a heartbeat, and the
  supervisor restart-loops it against the burst budget with the real
  reason buried in its stderr stream).
* End to end: a child spawned by the supervisor's own ``_spawn_child``
  resolves the ref IN ITS OWN PROCESS and acquires its Postgres
  connections through the provider — the ref crosses the spawn envelope
  as a resolvable string, never a serialized provider object (pinned
  against the live database by the integration test at the bottom).
"""

import asyncio
import contextlib
from pathlib import Path

import pytest

from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.workgroup import WorkerSpec, WorkgroupConfig


def test_worker_spec_forwards_credential_provider_to_cli_args() -> None:
    """``WorkerSpec(pg_credential_provider=...)`` must appear in the child
    CLI args as ``--pg-credential-provider module:attr`` — the flag the
    child CLI already parses."""
    try:
        spec = WorkerSpec(
            name="w1",
            queues=["default"],
            pg_credential_provider="infra.identity:pg_credentials",
        )
    except TypeError as exc:
        pytest.fail(
            f"WorkerSpec has no credential-provider field: {exc}. "
            "worker_main and the worker CLI both accept a Postgres "
            "credential provider, but the workgroup supervisor has no seam "
            "to pass one to its children — a workgroup whose workers need "
            "per-worker providers (Entra/managed identity) cannot be "
            "adopted."
        )

    args = spec.cli_args()
    assert "--pg-credential-provider" in args, (
        f"cli_args() does not emit --pg-credential-provider: {args}. The "
        "child command is assembled from a fixed prefix plus cli_args(), "
        "so a field cli_args() does not emit never reaches the child."
    )
    value = args[args.index("--pg-credential-provider") + 1]
    assert value == "infra.identity:pg_credentials"


def test_workgroup_toml_propagates_per_worker_credential_provider(
    tmp_path: Path,
) -> None:
    """Two workers with DIFFERENT providers must each get their own — the
    case environment inheritance (a single fleet-wide
    ``TASKQ_PG_CREDENTIAL_PROVIDER``) cannot express."""
    config = tmp_path / "workgroup.toml"
    config.write_text(
        """
actors = "tests.actors:registry"

[[workers]]
name = "ingest"
queues = ["ingest"]
pg_credential_provider = "infra.identity:ingest_credentials"

[[workers]]
name = "reports"
queues = ["reports"]
pg_credential_provider = "infra.identity:reports_credentials"
"""
    )

    cfg = WorkgroupConfig.from_toml(config)
    by_name = {w.name: w for w in cfg.workers}

    for name, expected in (
        ("ingest", "infra.identity:ingest_credentials"),
        ("reports", "infra.identity:reports_credentials"),
    ):
        args = by_name[name].cli_args()
        assert "--pg-credential-provider" in args, (
            f"worker {name!r}: pg_credential_provider from the TOML was "
            f"silently dropped — cli_args() is {args}. The loader never "
            "reads the key and WorkerSpec has no field for it, so nothing "
            "reaches the child process."
        )
        assert args[args.index("--pg-credential-provider") + 1] == expected


def test_workgroup_toml_rejects_an_empty_credential_provider(tmp_path: Path) -> None:
    """``pg_credential_provider = ""`` must fail at LOAD time, naming the
    worker and the field.

    An empty string passes the loader's optional-str type check and is
    forwarded as ``--pg-credential-provider ""``, so the child dies at
    import-ref resolution — before it can register a heartbeat or drain a
    queue — and the supervisor restart-loops it against the burst budget,
    with the only diagnostic buried in the child's stderr stream.
    """
    config = tmp_path / "workgroup.toml"
    config.write_text(
        """
actors = "tests.actors:registry"

[[workers]]
name = "ingest"
queues = ["ingest"]
pg_credential_provider = ""
"""
    )

    with pytest.raises(ValueError, match=r"worker\['ingest'\].pg_credential_provider") as excinfo:
        WorkgroupConfig.from_toml(config)

    assert "module:attr" in str(excinfo.value), (
        "the error must state what a provider ref IS (module:attr), not "
        f"only that this value is not one: {excinfo.value}"
    )


# ── End-to-end: the spawned child resolves and invokes the provider ─────


@pytest.mark.integration
async def test_spawned_child_acquires_connections_through_the_provider(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A child spawned the workgroup's way — ``--pg-credential-provider
    module:attr`` on its command line, no ``env=`` override — must resolve
    the ref IN THE CHILD and acquire its Postgres connections THROUGH the
    provider (children need the credential at connect
    time, so the Entra/managed-identity segment can adopt workgroups).

    The child's inherited ``TASKQ_PG_DSN`` is stripped of its userinfo, so
    the provider is the ONLY route to a working credential: a child that
    silently fell back to a static DSN password could not authenticate and
    would never register. Readiness therefore proves both halves at once —
    the child booted (a fresh workers-row heartbeat) AND the spy provider
    was invoked under the child's own pid (recorded to a file only the
    child process writes). The provider ref crosses the spawn envelope as
    a resolvable ``module:attr`` string, never a serialized provider
    object: the spy is constructed in the child, and every recorded call
    carries the child's pid, not this test process's.
    """
    import os
    from urllib.parse import urlparse, urlunparse

    import asyncpg

    from taskq._ids import new_uuid
    from taskq.worker.workgroup import _ChildState, _spawn_child

    schema = module_pg_schema.schema_name

    # The passwordless DSN the child inherits: same server/database, no
    # userinfo, sslmode=disable (the documented test-container opt-out of
    # the provider path's ensure_sslmode_require — the container runs no
    # TLS). The spy provider hands back exactly the userinfo this strip
    # removed, so authentication succeeds ONLY through it.
    parsed = urlparse(module_pg_schema.pg_dsn)
    user = parsed.username or ""
    password = parsed.password or ""
    no_userinfo = parsed.netloc.split("@")[-1]
    # urlunparse adds the '?' separator itself, so the query carries bare
    # params only — a leading '?' here would yield '??sslmode=...' and the
    # first '?' would become part of the parameter name.
    query = "sslmode=disable" if not parsed.query else f"{parsed.query}&sslmode=disable"
    dsn_no_credentials = urlunparse(parsed._replace(netloc=no_userinfo, query=query))

    calls_file = tmp_path / "provider_calls.txt"
    (tmp_path / "probe_actors.py").write_text(
        "from taskq.actor import actor\n"
        "from pydantic import BaseModel\n\n\n"
        "class ProbePayload(BaseModel):\n"
        "    value: int = 0\n\n\n"
        "@actor(name='workgroup_provider_probe', queue='default')\n"
        "async def probe(payload: ProbePayload) -> None:\n"
        "    return None\n\n\n"
        "ACTORS = {probe.name: probe}\n"
    )
    (tmp_path / "spy_provider.py").write_text(
        "import os\n"
        "from pathlib import Path\n\n"
        "from taskq.auth import PgCredential\n\n\n"
        "_CALLS = Path(__file__).with_name('provider_calls.txt')\n\n\n"
        "class _SpyProvider:\n"
        "    async def get_pg_credential(self) -> PgCredential:\n"
        "        with _CALLS.open('a') as fh:\n"
        "            fh.write(f'called pid={os.getpid()}\\n')\n"
        f"        return PgCredential(username={user!r}, password={password!r})\n\n\n"
        "PROVIDER = _SpyProvider()\n"
    )

    monkeypatch.setenv("TASKQ_PG_DSN", dsn_no_credentials)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)
    monkeypatch.delenv("TASKQ_PG_CREDENTIAL_PROVIDER", raising=False)
    # The child imports the probe registry and the spy from tmp_path.
    monkeypatch.setenv("PYTHONPATH", os.fspath(tmp_path))

    child = _ChildState(
        spec=WorkerSpec(
            name="provider-probe",
            queues=["default"],
            poll_interval=0.5,
            max_concurrency=2,
            pg_credential_provider="spy_provider:PROVIDER",
        )
    )
    wg_instance = new_uuid()
    await _spawn_child(child, "probe_actors:ACTORS", wg_instance)
    proc = child.process
    stream_tasks = [t for t in (child.stdout_task, child.stderr_task) if t is not None]
    assert proc is not None, "spawn produced no process"

    deadline = asyncio.get_running_loop().time() + 45.0
    registered = False
    provider_pids: set[int] = set()
    try:
        while asyncio.get_running_loop().time() < deadline:
            if proc.returncode is not None:
                pytest.fail(
                    f"workgroup child exited rc={proc.returncode} before "
                    "registering through the credential provider — its "
                    "output is in this test's captured logs "
                    "(workgroup.child_output lines), and with a "
                    "passwordless TASKQ_PG_DSN any boot failure means a "
                    "pool did NOT authenticate through the provider."
                )
            if calls_file.exists():
                provider_pids = {
                    int(line.rsplit("=", 1)[1])
                    for line in calls_file.read_text().splitlines()
                    if line.startswith("called pid=")
                }
            conn = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                row = await conn.fetchrow(
                    f'SELECT pid FROM "{schema}".workers '  # noqa: S608  # Why: schema is the fixture's own hashed name, validated by the migration runner's _IDENT_RE; pid is $-bound.
                    "WHERE worker_label = $1 AND last_seen_at > now() - interval '10 seconds'",
                    child.spec.name,
                )
            finally:
                await conn.close()
            if row is not None and provider_pids:
                registered = True
                break
            await asyncio.sleep(0.25)

        assert registered, (
            "the spawned child never registered a fresh heartbeat within "
            "45s — with a passwordless TASKQ_PG_DSN it cannot authenticate "
            "unless every Postgres pool is acquired through the "
            "pg_credential_provider."
        )
        child_pid = proc.pid
        assert child_pid is not None
        assert provider_pids == {child_pid}, (
            f"the credential provider must be invoked in the child's own "
            f"process (expected pid {child_pid}, saw {sorted(provider_pids)}): "
            "a provider serialized from the supervisor, or a pool that fell "
            "back to the DSN, breaks the per-worker provider contract."
        )
    finally:
        if proc.returncode is None:
            # The returncode stays None until the loop reaps the exit, so a
            # child that exited under a loaded runner reaches terminate()
            # already dead. The supervisor's own _kill_child guards the
            # same race with the same suppressions.
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10.0)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()
        # Reap the stream pumps the way the supervisor's liveness monitor
        # does, so no cancelled task is left un-awaited.
        for t in stream_tasks:
            if not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t
