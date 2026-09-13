"""Red-team pins for workgroup → child credential-provider forwarding.

``worker_main`` accepts ``pg_credential_provider``, the per-slot pool is
provider-backed when one is given, and the ``taskq worker`` CLI resolves
``--pg-credential-provider module:attr`` itself. But the supervisor cannot
forward it: ``WorkerSpec`` (``worker/workgroup.py:118-129``) has no
credential-provider field, ``cli_args()`` (``:131-141``) emits no such
flag, and the child command is a fixed prefix plus ``cli_args()``
(``:452-466``) — no seam through which a provider reaches a child.

Children spawned without ``env=`` do inherit
``TASKQ_PG_CREDENTIAL_PROVIDER``, but that covers a single fleet-wide
provider only. A workgroup whose workers need *different* providers — or
any caller configuring providers programmatically — has no route at all,
which is what blocks the Entra/managed-identity segment from adopting
workgroups.

The contract these tests pin: a per-worker Postgres credential provider
declared on a ``WorkerSpec`` (directly or via the TOML loader) is
forwarded to that worker's child command line.
"""

from pathlib import Path

import pytest

from taskq.worker.workgroup import WorkerSpec, WorkgroupConfig


def test_worker_spec_forwards_credential_provider_to_cli_args() -> None:
    """``WorkerSpec(pg_credential_provider=...)`` must appear in the child
    CLI args as ``--pg-credential-provider module:attr`` — the flag the
    child CLI already parses."""
    try:
        spec = WorkerSpec(
            name="w1",
            queues=["default"],
            pg_credential_provider="infra.identity:pg_credentials",  # pyright: ignore[reportCallIssue]  # Why: the field does not exist yet — that absence IS the defect this red test pins. The TypeError caught below is the assertion; this call must keep the desired shape so the ignore disappears when the field lands.
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
