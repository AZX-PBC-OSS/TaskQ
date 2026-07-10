"""Actor configuration carrier dataclass for worker-startup config sync."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ActorConfig:
    """Carrier for a registered actor's persisted configuration row.

    Constructed from ``ActorRef`` fields at worker startup and passed to
    ``sync_actor_config`` for the two-phase upsert into ``{schema}.actor_config``.
    """

    actor: str
    max_concurrent: int | None
    queue: str
    max_pending: int | None = None
    result_ttl: float | None = None
    metadata: dict[str, object] = field(default_factory=dict[str, object])
