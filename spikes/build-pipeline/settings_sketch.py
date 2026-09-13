"""Validation sketch for the TS-runtime worker config (spike).

Mirrors ``src/taskq/settings.py`` (``WorkerSettings``) conventions —
declarative fields with defaults and bounds, validator hooks carrying
"Why:" notes, a ``load()`` classmethod, and ``TASKQ_*`` env names documented
per field — but reads TOML via :mod:`tomllib` and validates with pydantic v2
(the sketch language the spike was asked for; production would use the
dotenvmodel ``DotEnvConfig`` style of the real settings module, and env
parsing only).

Run: ``uv run python spikes/build-pipeline/settings_sketch.py``
Validates the example TOML, then proves the invariant checks fail closed on
bad config (queue overlap across pools, blank command, empty queues).
"""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

HERE = Path(__file__).parent


class RuntimePool(BaseModel):
    """One named pool of warm runtime processes serving explicit queues."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(
        description="Pool name; used in logs, metrics labels and the admin UI.",
    )
    queues: list[str] = Field(
        description="Queues served by this pool. Each queue may belong to at "
        "most one pool (cross-pool overlap is rejected by RuntimeConfig).",
    )
    max_pool_size: int = Field(
        default=8,
        ge=1,
        le=256,
        description="TASKQ_RUNTIME_MAX_POOL_SIZE. Upper bound on live runtime "
        "processes in this pool. Bounds process count, not job parallelism — "
        "the runtime enforces per-actor maxConcurrent internally.",
    )
    idle_exit_timeout: float = Field(
        default=60.0,
        ge=0.0,
        description="TASKQ_RUNTIME_IDLE_EXIT_TIMEOUT (seconds). Reap a "
        "runtime process after this long with zero in-flight jobs. 0 keeps "
        "processes for the worker's lifetime (cheap cold starts make this "
        "attractive; memory decides).",
    )

    @model_validator(mode="after")
    def _pool_has_queues(self) -> "RuntimePool":
        # An empty-queues pool would consume max_pool_size processes while
        # never dispatching; drop it at load rather than in dispatch.
        if not self.queues:
            raise ValueError(f"pool {self.name!r} lists no queues")
        if len(set(self.queues)) != len(self.queues):
            raise ValueError(f"pool {self.name!r} lists duplicate queues: {self.queues}")
        return self


class RuntimeConfig(BaseModel):
    """``[runtime]`` table: entrypoint fields plus the per-queue pools.

    Why pools live under ``runtime``: TOML's ``[[runtime.pools]]`` nests
    them there, and the entrypoint + its pools are configured together —
    every pool spawns the same entrypoint.
    """

    model_config = ConfigDict(frozen=True)

    command: str = Field(
        description="TASKQ_RUNTIME_COMMAND. Executable to spawn. Required, "
        "non-empty: the worker refuses to start a TS pool without an explicit "
        "entrypoint (fail closed).",
    )
    args: list[str] = Field(
        default_factory=list,
        description="TASKQ_RUNTIME_ARGS. Full argv after the command.",
    )
    cwd: str = Field(
        default=".",
        description="TASKQ_RUNTIME_CWD. Spawn working directory; relative "
        "entrypoint paths resolve against this.",
    )
    protocol: int = Field(
        default=1,
        ge=1,
        description="TASKQ_RUNTIME_PROTOCOL. Expected NDJSON stdio protocol "
        "version; the worker aborts on boot-frame mismatch.",
    )
    boot_timeout: float = Field(
        default=10.0,
        ge=0.5,
        description="TASKQ_RUNTIME_BOOT_TIMEOUT (seconds). Spawn -> "
        "__TASKQ_READY__ budget; non-zero exit on timeout.",
    )
    pools: list[RuntimePool] = Field(min_length=1)

    @field_validator("command")
    @classmethod
    def _command_not_blank(cls, value: str) -> str:
        # A blank command would only die at spawn time with a confusing
        # FileNotFoundError buried in worker startup; surface it at load.
        if not value.strip():
            raise ValueError("runtime.command must be a non-empty executable name or path")
        return value

    @model_validator(mode="after")
    def _queues_resolve_to_one_pool(self) -> "RuntimeConfig":
        # Why a cross-pool validator instead of per-pool checks: the invariant
        # is over the *set* of pools (a queue must resolve to exactly one
        # runtime pool), which no single pool can see. Like WorkerSettings'
        # ``lock_lease >= 4 * heartbeat_interval``, the check runs at load so
        # a bad deploy fails fast instead of double-dispatching at runtime.
        owner: dict[str, str] = {}
        for pool in self.pools:
            for queue in pool.queues:
                if queue in owner:
                    raise ValueError(
                        f"queue {queue!r} is claimed by both pool "
                        f"{owner[queue]!r} and pool {pool.name!r}; each queue "
                        "must resolve to exactly one runtime pool"
                    )
                owner[queue] = pool.name
        return self


class TaskqWorkerToml(BaseModel):
    """Top-level ``taskq.worker.toml`` document."""

    runtime: RuntimeConfig

    @classmethod
    def load(cls, path: Path) -> "TaskqWorkerToml":
        """Load and validate a worker TOML file."""
        with path.open("rb") as fh:
            data = tomllib.load(fh)
        return cls.model_validate(data)


def main() -> None:
    print("== validating taskq.worker.example.toml (expect success)")
    config = TaskqWorkerToml.load(HERE / "taskq.worker.example.toml")
    print(f"   entrypoint: {config.runtime.command} {' '.join(config.runtime.args)}")
    for pool in config.runtime.pools:
        print(
            f"   pool {pool.name}: queues={pool.queues} "
            f"max_pool_size={pool.max_pool_size} "
            f"idle_exit_timeout={pool.idle_exit_timeout}s"
        )

    print("\n== invariant checks (expect ValidationError)")
    bad_overlap = {
        "runtime": {
            "command": "node",
            "args": ["dist/runtime.bundle.mjs"],
            "pools": [
                {"name": "a", "queues": ["emails", "media"]},
                {"name": "b", "queues": ["media"]},  # 'media' in two pools
            ],
        },
    }
    for label, data in (
        ("queue overlap across pools", bad_overlap),
        (
            "blank command",
            {
                "runtime": {
                    "command": "  ",
                    "args": ["runtime.ts"],
                    "pools": [{"name": "a", "queues": ["default"]}],
                },
            },
        ),
        (
            "empty queues list",
            {
                "runtime": {
                    "command": "node",
                    "pools": [{"name": "a", "queues": [], "idle_exit_timeout": 60.0}],
                },
            },
        ),
    ):
        try:
            TaskqWorkerToml.model_validate(data)
        except ValidationError as exc:
            first = str(exc.errors()[0]["msg"])
            print(f"   {label}: REJECTED ({first})")
        else:  # pragma: no cover - assertion guard
            raise AssertionError(f"{label} was not rejected")


if __name__ == "__main__":
    main()
