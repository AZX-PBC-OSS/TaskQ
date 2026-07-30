"""ActorsClient — pool-wrapping facade for actor configuration operations.

Provides a typed surface for listing, inspecting, tuning, and deregistering
stored ``actor_config`` rows. Each method acquires a connection from the
injected pool, delegates to ``taskq.actor_config_ops``, and returns
the result.
"""

from typing import TYPE_CHECKING

from taskq.actor_config_ops import (
    UNSET,
    ActorConfigRow,
    DeregisterResult,
    Unset,
    deregister_actor,
    get_actor_config,
    list_actor_configs,
    set_actor_config_capacity,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = ["ActorsClient"]


class ActorsClient:
    """Pool-wrapping facade for actor configuration operations.

    Acquires a connection from the injected pool for each call, delegates
    to ``taskq.actor_config_ops``, and returns the result. The
    caller must have opened the pool; this class does not manage its
    lifecycle.

    Parameters
    ----------
    pool:
        An open ``asyncpg.Pool``. The caller retains ownership.
    schema:
        TaskQ schema name. Defaults to ``"taskq"``.
    """

    def __init__(self, pool: "asyncpg.Pool", *, schema: str = "taskq") -> None:
        self._pool = pool
        self._schema = schema

    async def list(self) -> list[ActorConfigRow]:
        """List all stored actor_config rows, ordered by actor name."""
        async with self._pool.acquire() as conn:
            return await list_actor_configs(conn, schema=self._schema)

    async def get(self, actor: str) -> ActorConfigRow | None:
        """Get one actor_config row, or ``None`` if not found."""
        async with self._pool.acquire() as conn:
            return await get_actor_config(conn, actor, schema=self._schema)

    async def set_capacity(
        self,
        actor: str,
        *,
        max_concurrent: int | None | Unset = UNSET,
        max_pending: int | None | Unset = UNSET,
        result_ttl: float | None | Unset = UNSET,
    ) -> ActorConfigRow | None:
        """Update capacity fields on an existing actor_config row."""
        async with self._pool.acquire() as conn:
            return await set_actor_config_capacity(
                conn,
                actor,
                max_concurrent=max_concurrent,
                max_pending=max_pending,
                result_ttl=result_ttl,
                schema=self._schema,
            )

    async def deregister(
        self,
        actor: str,
        *,
        force: bool = False,
        purge_queue: bool = False,
    ) -> DeregisterResult:
        """Deregister an actor with safety checks.

        See :func:`taskq.actor_config_ops.deregister_actor` for
        the full semantics.
        """
        async with self._pool.acquire() as conn:
            return await deregister_actor(
                conn,
                actor,
                force=force,
                purge_queue=purge_queue,
                schema=self._schema,
            )
