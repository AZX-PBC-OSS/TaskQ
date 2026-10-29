"""Shared drill plumbing: schema-per-drill setup and the capture helper.

Each drill runs against its OWN schema (fresh migrations, no cross-drill
state) and captures its receipt to stdout — the run's output IS the
evidence (redirect it into ``.measurements/``).
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import taskq.migrate

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


def dsn() -> str:
    return os.environ["TASKQ_PG_DSN"]


def use_schema(slug: str) -> str:
    """Compute + provision a fresh schema for ONE drill and point the
    pipeline module's env at it. Call BEFORE importing ``pipeline`` — the
    module reads the environment at import time (the deps door binds
    once)."""
    base = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    schema = f"{base}_probe_{slug}"
    os.environ["TASKQ_SCHEMA_NAME"] = schema
    return schema


async def provision(schema: str) -> None:
    await taskq.migrate.apply_pending_locked(dsn(), schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn(), schema=schema, phase="post")


def run(drill: Callable[[], Awaitable[None]]) -> None:
    asyncio.run(drill())
