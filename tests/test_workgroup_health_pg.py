"""C9: workgroup health verdicts must track the SERVER age of last_seen_at.

The supervisor's own wall clock must not participate: ``last_seen_at`` is
written by PG (``clock_timestamp()``), so only PG can measure its age
without mixing domains.  Pre-fix, ``_child_health_check`` computed the age
from ``time.time()`` — a supervisor whose clock ran ahead read a healthy
child as stale and killed it.
"""

import asyncio
import time
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from uuid import UUID, uuid4

import asyncpg
import pytest

from taskq.settings import TaskQSettings
from taskq.worker.workgroup import (
    WorkerHealthConfig,
    WorkerSpec,
    _child_health_check,
    _ChildState,
)

pytestmark = pytest.mark.integration


async def _seed_worker_row(
    pool: asyncpg.Pool, schema: str, *, age: timedelta
) -> tuple[int, str, UUID]:
    """Insert a workers row whose ``last_seen_at`` is *age* old by the SERVER
    clock, in its own fresh workgroup instance; return (pid, label, instance)."""
    wg_instance, label, pid = uuid4(), "w1", 4242
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers '  # noqa: S608  # Why: schema validated against _IDENT_RE by the module fixtures.
            "(id, workgroup_instance, worker_label, pid, hostname, queues, last_seen_at) "
            "VALUES ($1, $2, $3, $4, 'supervisor-test', ARRAY['default'], clock_timestamp() - $5::interval)",
            uuid4(),
            wg_instance,
            label,
            pid,
            age,
        )
    return pid, label, wg_instance


async def _verdict(
    pool: asyncpg.Pool,
    schema: str,
    pid: int,
    label: str,
    wg_instance: UUID,
    stale_after: float,
    monkeypatch: pytest.MonkeyPatch,
) -> bool:
    """Run one health check with the supervisor's Python clock skewed a full
    day ahead — pre-fix this inflates the age by 86400 s and kills a healthy
    child."""
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 86_400)
    child = _ChildState(spec=WorkerSpec(name=label, queues=["q"]))
    # Why: _child_health_check reads child.process.pid for the identity check
    # (workgroup.py); a bare pid-carrying stub satisfies exactly that read.
    child.process = cast(asyncio.subprocess.Process, SimpleNamespace(pid=pid))
    return await _child_health_check(
        child, pool, schema, WorkerHealthConfig(enabled=True, stale_after=stale_after), wg_instance
    )


async def test_healthy_child_survives_python_clock_skew(
    module_pg_pool: asyncpg.Pool,
    settings: TaskQSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child whose server-side age (30 s) is well under stale_after (60 s)
    must read healthy even when the supervisor's clock is a day ahead."""
    pid, label, wg_instance = await _seed_worker_row(
        module_pg_pool, settings.schema_name, age=timedelta(seconds=30)
    )
    verdict = await _verdict(
        module_pg_pool,
        settings.schema_name,
        pid,
        label,
        wg_instance,
        stale_after=60.0,
        monkeypatch=monkeypatch,
    )
    assert verdict is True


async def test_stale_child_flagged_with_python_clock_skew(
    module_pg_pool: asyncpg.Pool,
    settings: TaskQSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child whose server-side age (90 s) exceeds stale_after (60 s) must
    be flagged unhealthy regardless of the supervisor's clock skew."""
    pid, label, wg_instance = await _seed_worker_row(
        module_pg_pool, settings.schema_name, age=timedelta(seconds=90)
    )
    verdict = await _verdict(
        module_pg_pool,
        settings.schema_name,
        pid,
        label,
        wg_instance,
        stale_after=60.0,
        monkeypatch=monkeypatch,
    )
    assert verdict is False
