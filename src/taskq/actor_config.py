"""Actor configuration carrier dataclass for worker-startup config sync."""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Final, Literal

MAX_ACTOR_RETENTION_DAYS: Final[int] = timedelta.max.days
"""Maximum supported ``actor_config.metadata.retention_days`` value."""
_MAX_ACTOR_RETENTION_DAYS_TEXT = str(MAX_ACTOR_RETENTION_DAYS)

type CapacityOwner = Literal["operator", "code"]
"""Who owns an actor's capacity columns once an ``actor_config`` row exists.

``"operator"`` (the default — today's law, unchanged): the stored row wins
on every conflict; the ``@actor(...)`` literal only seeds the row on first
creation, and a later literal change is the expected operator-override
log, never a write.

``"code"`` (the opt-in): the declaration of record is the CODE literal —
the startup upsert's ``DO UPDATE SET`` rewrites ``max_concurrent`` and
``result_ttl`` from the literal on every boot (the retry contract's own
law, on the capacity axes), so a changed literal deploys by code instead
of silently doing nothing after first boot.
"""

CAPACITY_OWNER_OPERATOR: Final[CapacityOwner] = "operator"
CAPACITY_OWNER_CODE: Final[CapacityOwner] = "code"
_CAPACITY_OWNERS: Final[tuple[str, ...]] = (CAPACITY_OWNER_OPERATOR, CAPACITY_OWNER_CODE)


def validate_capacity_owner(value: object, what: str) -> None:
    """The declaration-shape door both actor surfaces share: *value* must
    be exactly one of the two owner names — anything else (another
    string, ``None``, a bool, an int) is the refused declaration, raised
    as ``ValueError`` naming the declaration site."""
    if type(value) is not str or value not in _CAPACITY_OWNERS:
        raise ValueError(
            f"{what} capacity_owner must be 'operator' (the default: the "
            f"operator owns the stored capacity once a row exists) or "
            f"'code' (the literal deploys by code on every boot); got "
            f"{value!r}."
        )


def normalize_retention_days(value: object) -> int:
    """Validate and normalize a per-actor retention override."""
    if type(value) is int:
        days = value
    elif type(value) is str and value.isascii() and value.isdigit():
        normalized = value.lstrip("0") or "0"
        if len(normalized) > len(_MAX_ACTOR_RETENTION_DAYS_TEXT) or (
            len(normalized) == len(_MAX_ACTOR_RETENTION_DAYS_TEXT)
            and normalized > _MAX_ACTOR_RETENTION_DAYS_TEXT
        ):
            days = -1
        else:
            days = int(normalized)
    else:
        days = -1

    if not 0 <= days <= MAX_ACTOR_RETENTION_DAYS:
        raise ValueError(
            "metadata.retention_days must be an integer or ASCII digit string "
            f"from 0 through {MAX_ACTOR_RETENTION_DAYS}"
        )
    return days


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
    # The declared retry curve, seeded on the row's first create so the
    # server-side enqueue paths (cron fires, admin run-now) re-pend on the
    # curve the actor declared. None never reaches here (the ref's retry
    # is a full RetryPolicy); the COLUMNS are nullable for the rows that
    # predate the migration.
    retry_base: timedelta | None = None
    retry_cap: timedelta | None = None
    retry_backoff: str | None = None
    retry_jitter: float | None = None
    # The declared retry contract. Unlike the curve above these two are
    # code-owned on every boot, not seed-only: the upsert's ON CONFLICT
    # arm rewrites them from the literal, because they decide how many
    # attempts a server-side fire gets and which retry family it belongs
    # to, and a stale first-registration value there silently gives every
    # cron-fired job the wrong retry contract. Defaults mirror the
    # RetryPolicy defaults so a carrier built without them (test doubles,
    # harness seeds) still round-trips; the bootstrap always passes the
    # ref's validated values explicitly.
    max_attempts: int = 3
    retry_kind: str = "transient"
    # THE CAPACITY OWNER (C15's opt-in): who owns the capacity columns
    # once this row exists. "operator" (the default) is today's law —
    # the stored value survives every subsequent upsert untouched.
    # "code" hands max_concurrent/result_ttl to the declaration: the
    # upsert's SET clause rewrites them from THIS carrier's literal on
    # every boot. The queue and max_pending stay operator-owned for BOTH
    # owners (the queue's move-queue law; max_pending's enforcement is
    # the producer-side enqueue admission reading the stored row).
    # Plain ``str`` (the retry_kind shape): the two owner names are the
    # decorators' static door (``CapacityOwner``) + their shared runtime
    # validation; the carrier just carries.
    capacity_owner: str = "operator"

    def __post_init__(self) -> None:
        if "retention_days" in self.metadata:
            normalize_retention_days(self.metadata["retention_days"])
