"""Tests for TaskQ.actors property and public API exports."""

from __future__ import annotations

import pytest

from taskq.testing.fixtures import ModulePgSchema


def test_actors_client_importable_from_taskq() -> None:
    from taskq import ActorsClient

    assert ActorsClient is not None


def test_deregister_result_importable_from_taskq() -> None:
    from taskq import DeregisterResult

    assert DeregisterResult is not None


def test_deregistration_exceptions_importable_from_taskq() -> None:
    from taskq import (
        ActorDeregistrationError,
        ActorHasActiveJobsError,
        ActorHasEnabledSchedulesError,
        ActorNotFoundError,
    )

    assert ActorDeregistrationError is not None
    assert ActorHasActiveJobsError is not None
    assert ActorHasEnabledSchedulesError is not None
    assert ActorNotFoundError is not None


def test_taskq_actors_raises_before_open() -> None:
    """Accessing tq.actors before open() raises RuntimeError."""
    from taskq import TaskQ

    tq = TaskQ(dsn="postgresql://fake:fake@localhost/fake")
    with pytest.raises(RuntimeError, match="not open"):
        _ = tq.actors


@pytest.mark.asyncio
@pytest.mark.integration
async def test_taskq_actors_property_returns_actors_client(
    module_pg_schema: ModulePgSchema,
) -> None:
    """TaskQ.actors returns an ActorsClient bound to the same pool and schema."""
    from taskq import TaskQ
    from taskq.client._actors import ActorsClient

    async with TaskQ(
        dsn=module_pg_schema.pg_dsn,
        schema=module_pg_schema.schema_name,
    ) as tq:
        client = tq.actors
        assert isinstance(client, ActorsClient)
